import express from 'express';
import { Server } from 'http';
import { Server as HttpsServer } from 'https';
import { readFileSync } from 'fs';
import { join } from 'path';
import socketIO from 'socket.io';
import Tracer from 'tracer';
import morgan from 'morgan';
import peerConfig from './peerConfig';
import { ICEServer } from './ICEServer';
let TurnServer = require('node-turn');

const httpsEnabled = !!process.env.HTTPS;

const port = process.env.PORT || (httpsEnabled ? '443' : '9736');

const sslCertificatePath = process.env.SSLPATH || process.cwd();

const logger = Tracer.colorConsole({
	format: '{{timestamp}} <{{title}}> {{message}}',
});

const turnLogger = Tracer.colorConsole({
	format: '{{timestamp}} <{{title}}> <ice> {{message}}',
	level: peerConfig.integratedRelay.debugLevel.toLowerCase(),
});

const app = express();
let server: HttpsServer | Server;
if (httpsEnabled) {
	server = new HttpsServer(
		{
			key: readFileSync(join(sslCertificatePath, 'privkey.pem')),
			cert: readFileSync(join(sslCertificatePath, 'fullchain.pem')),
		},
		app
	);
} else {
	server = new Server(app);
}

let turnServer: any | null = null;
if (peerConfig.integratedRelay.enabled) {
	turnServer = new TurnServer({
		listeningIps: peerConfig.integratedRelay.listeningIps,
		relayIps: peerConfig.integratedRelay.relayIps,
		externalIps: peerConfig.integratedRelay.externalIps,
		minPort: peerConfig.integratedRelay.minPort,
		maxPort: peerConfig.integratedRelay.maxPort,
		listeningPort: peerConfig.integratedRelay.listeningPort,
		authMech: 'long-term',
		debugLevel: peerConfig.integratedRelay.debugLevel,
		realm: 'crewlink',
		debug: (level: string, message: string) => {
			turnLogger[level.toLowerCase()](message);
		},
	});

	turnServer.addUser(peerConfig.integratedRelay.defaultUsername, peerConfig.integratedRelay.defaultPassword);

	turnServer.start();
}

const io = socketIO(server);
const clients = new Map<string, Client>();
const publicLobbies = new Map<string, PublicLobby>();
const lobbyCodes = new Map<number, string>();
const allLobbies = new Map<string, lobbyInfo>();
let lobbyCount = 0;

function removePublicLobby(c: string) {
	if (publicLobbies.has(c)) {
		let pid = publicLobbies.get(c).id;
		io.sockets.in('lobbybrowser').emit('remove_lobby', pid);
		lobbyCodes.delete(pid);
		publicLobbies.delete(c);
	}
}
interface Client {
	playerId: number;
	clientId: number;
}

interface Signal {
	data: string;
	to: string;
}

interface ClientPeerConfig {
	forceRelayOnly: boolean;
	iceServers: ICEServer[];
}

app.enable('trust proxy');
app.set('views', join(__dirname, '../views'));
app.use('/public', express.static(join(__dirname, '../public')));
app.set('view engine', 'pug');
app.use(morgan('combined'));

let connectionCount = 0;

let hostname = process.env.HOSTNAME;
if (!hostname && peerConfig.integratedRelay.enabled) {
	logger.error('You must set the HOSTNAME environment variable to use the TURN server.');
	process.exit(1);
}

app.get('/', (req, res) => {
	let address = req.protocol + '://' + req.host;
	res.render('index', { connectionCount, address, lobbiesCount: allLobbies.size });
});

app.get('/health', (req, res) => {
	let address = req.protocol + '://' + req.host;
	res.json({
		uptime: process.uptime(),
		connectionCount,
		lobbiesCount: allLobbies.size,
		address,
		name: process.env.NAME,
	});
});

app.get('/lobbies', (req, res) => {
	res.json(Array.from(publicLobbies.values()));
});

const leaveroom = (socket: socketIO.Socket, code: string) => {
	if (!code) {
		return;
	}
	if (code && code.length === 6) socket.leave(code);

	if ((io.sockets.adapter.rooms[code]?.length ?? 0) <= 0) {
		if (allLobbies.has(code)) {
			allLobbies.delete(code);
		}
		removePublicLobby(code);
	}
};
io.on('connection', (socket: socketIO.Socket) => {
	connectionCount++;
	logger.info('Total connected: %d in %d lobbies', connectionCount, allLobbies.size);
	let code: string | null = null;

	const clientPeerConfig: ClientPeerConfig = {
		forceRelayOnly: peerConfig.forceRelayOnly,
		iceServers: peerConfig.iceServers ? [...peerConfig.iceServers] : [],
	};

	if (turnServer) {
		//	const turnCredential = crypto.randomBytes(32).toString('base64');
		//	turnServer.addUser(socket.id, turnCredential);
		// logger.info(`Adding socket "${socket.id}" as TURN user.`);
		clientPeerConfig.iceServers.push({
			urls: `turn:${hostname}:${peerConfig.integratedRelay.listeningPort}`,
			username: peerConfig.integratedRelay.defaultUsername,
			credential: peerConfig.integratedRelay.defaultPassword,
		});
	}

	socket.emit('clientPeerConfig', clientPeerConfig);

	socket.on('join', (c: string, id: number, clientId: number, isHost?: boolean) => {
		if (
			typeof c !== 'string' ||
			typeof id !== 'number' ||
			typeof clientId !== 'number' ||
			!(c.length === 6 || c.length === 4)
		) {
			socket.disconnect();
			logger.error(`Socket %s sent invalid join command: %s %d %d`, socket.id, c, id, clientId);
			return;
		}

		let otherClients: any = {};
		if (io.sockets.adapter.rooms[c]) {
			let socketsInLobby = Object.keys(io.sockets.adapter.rooms[c].sockets);
			for (let s of socketsInLobby) {
				if (s !== socket.id) otherClients[s] = clients.get(s);
			}
		}

		if (!allLobbies.has(c)) {
			allLobbies.set(c, { code: c, hostId: isHost ? clientId : -1, publicLobbyId: -1, connectedCount: 1 });
		} else {
			allLobbies.get(c).connectedCount++;
			socket.emit('setHost', allLobbies.get(c).hostId);
		}

		if (code != c) leaveroom(socket, code);
		code = c;
		socket.join(code);
		socket.to(code).broadcast.emit('join', socket.id, {
			playerId: id,
			clientId: clientId,
		});
		socket.emit('setClients', otherClients);
		if (isHost) {
			socket.to(code).broadcast.emit('setHost', clientId);
		}
	});

	socket.on('setHost', (c: string, clientId: number) => {
		if (code === c) {
			if (allLobbies.has(c)) {
				allLobbies.get(c).hostId = clientId;
				socket.to(code).broadcast.emit('setHost', clientId);
			}
		}
	});

	socket.on('id', (id: number, clientId: number) => {
		if (typeof id !== 'number' || typeof clientId !== 'number') {
			socket.disconnect();
			logger.error(`Socket %s sent invalid id command: %d %d`, socket.id, id, clientId);
			return;
		}
		let client = clients.get(socket.id);
		if (client != null && client.clientId != null && client.clientId !== clientId) {
			///			socket.disconnect();
			logger.error(
				`Socket ${socket.id}->${client.clientId}->${clientId}->${id} sent invalid id command, attempted spoofing another client`
			);
			//			return;
		}
		client = {
			playerId: id,
			clientId: clientId,
		};
		clients.set(socket.id, client);
		socket.to(code).broadcast.emit('setClient', socket.id, client);
	});

	socket.on('leave', () => {
		if (code) {
			leaveroom(socket, code);
			clients.delete(socket.id); // @ts-ignore
		}
	});

	socket.on('VAD', (activity: boolean) => {
		let client = clients.get(socket.id);
		if (code && client) {
			socket.to(code).broadcast.emit('VAD', {
				activity,
				client,
				socketId: socket.id,
			});
		}
	});

	socket.on('join_lobby', (id: number, callbackFn) => {
		//ban check etc...
		if (lobbyCodes.has(id) && publicLobbies.has(lobbyCodes.get(id))) {
			let lobbyCode = lobbyCodes.get(id);
			let publicLobby = publicLobbies.get(lobbyCode);
			if (publicLobby.isPublic && publicLobby.gameState === GameState.LOBBY) {
				callbackFn(0, lobbyCode, publicLobby.server, publicLobby);
				return;
			} else {
				callbackFn(1, 'Lobby is not public anymore');
			}
		}
		callbackFn(1, 'Lobby not found :C');
	});

	socket.on('lobby', (c: string, lobbyInfo: PublicLobby) => {
		if (code != c) {
			logger.error(`Got request to host lobby while not in it %s`, c, code);
			return;
		}
		if (!lobbyInfo.isPublic && !lobbyInfo.isPublic2) {
			removePublicLobby(c);
		} else {
			const publobby = publicLobbies.has(c) ? publicLobbies.get(c) : undefined;
			const id = publobby ? publobby.id : lobbyCount++;
			const stateTime =
				publobby &&
				((publobby.gameState === GameState.LOBBY && lobbyInfo.gameState === GameState.LOBBY) ||
					(publobby.gameState !== GameState.LOBBY && lobbyInfo.gameState !== GameState.LOBBY))
					? publobby.stateTime
					: Date.now();
			let lobby: PublicLobby = {
				id,
				title: lobbyInfo.title?.substring(0, 20) ?? 'ERROR',
				host: lobbyInfo.host?.substring(0, 10) ?? '',
				current_players: lobbyInfo.current_players ?? 0,
				max_players: lobbyInfo.max_players ?? 0,
				language: lobbyInfo.language?.substring(0, 5) ?? '',
				mods: lobbyInfo.mods?.substring(0, 20)?.toUpperCase() ?? '',
				isPublic: lobbyInfo.isPublic || lobbyInfo.isPublic2,
				server: lobbyInfo.server,
				gameState: lobbyInfo.gameState,
				stateTime,
			};
			lobbyCodes.set(id, c);
			publicLobbies.set(c, lobby);
			io.sockets.in('lobbybrowser').emit('update_lobby', lobby);
		}
	});

	socket.on('remove_lobby', (c: string) => {
		if (code != c) {
			logger.error(`Got request to host lobby while not in it %s`, c, code);
			return;
		}
		removePublicLobby(c);
	});

	socket.on('signal', (signal: Signal) => {
		if (typeof signal !== 'object' || !signal.data || !signal.to || typeof signal.to !== 'string') {
			socket.disconnect();
			logger.error(`Socket %s sent invalid signal command: %j`, socket.id, signal);
			return;
		}
		const { to, data } = signal;
		io.to(to).emit('signal', {
			data,
			from: socket.id,
		});
	});

	socket.on('lobbybrowser', (open) => {
		if (!open) {
			socket.leave('lobbybrowser');
		} else {
			socket.join('lobbybrowser');
			io.sockets.in('lobbybrowser').emit('new_lobbies', Array.from(publicLobbies.values()));
		}
	});

	socket.on('disconnect', () => {
		leaveroom(socket, code);
		clients.delete(socket.id);
		connectionCount--;
		logger.info('Total connected: %d in %d lobbies', connectionCount, allLobbies.size);

		// if (turnServer) {
		// 	logger.info(`Removing socket "${socket.id}" as TURN user.`);
		// 	turnServer.removeUser(socket.id);
		// }
	});
});

server.listen(port);
logger.info('BetterCrewLink Server started: 127.0.0.1:%s', port);
