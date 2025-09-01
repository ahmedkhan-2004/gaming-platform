// Imports
var WebSocket = require('ws');
var fs = require("fs");

// GameServer implementation
function GameServer(confile) {
    // Startup
    this.nodesPlayer = []; // Nodes controlled by players
    this.movingNodes = []; // For move engine
    this.clients = [];
    this.nodes = [];

    // Config - ADD DEFAULTS HERE
    this.config = {
        "host": "localhost",
        "port": 8080,
        "maxConnections": 100,
        "gameMode": "default"
    };

    // Parse config
    this.loadConfig(confile);
    
    // Initialize WebSocket server as null
    this.wss = null;

    // Game state
    this.gameWidth = 21600;
    this.gameHeight = 21600;
    this.foods = [];
    this.snakes = new Map();
    
    // Initialize food
    this.initializeFood();
}

module.exports = GameServer;

// PROTOCOL TRANSLATOR - Convert between slither.io and your game protocol
GameServer.prototype.translateSlitherMessage = function(data) {
    if (data.length === 0) return null;
    
    const opcode = data[0];
    console.log(`\u001B[35m[Protocol]\u001B[0m Received slither.io message, opcode: ${opcode}`);
    
    switch (opcode) {
        case 115: // 's' - Initial setup request
            return { type: 'setup', data: data };
        case 13: // Mouse angle
            if (data.length >= 3) {
                const angle = (data[1] << 8) | data[2];
                return { type: 'angle', angle: angle * Math.PI / 32768 };
            }
            break;
        case 18: // Boost/speed up
            return { type: 'boost', active: true };
        case 19: // Stop boost
            return { type: 'boost', active: false };
        case 21: // Ping
            return { type: 'ping', data: data };
        case 112: // Alternative ping opcode
            return { type: 'ping', data: data };
        default:
            console.log(`\u001B[33m[Protocol]\u001B[0m Unknown slither.io opcode: ${opcode}`);
            return { type: 'unknown', opcode: opcode, data: data };
    }
    
    return null;
};

// Convert your game messages to slither.io protocol
GameServer.prototype.translateToSlither = function(gameMessage) {
    switch (gameMessage.type) {
        case 'initial_setup':
            return this.createInitialSetupMessage(gameMessage);
        case 'snake_update':
            return this.createSnakeUpdateMessage(gameMessage);
        case 'food_update':
            return this.createFoodUpdateMessage(gameMessage);
        case 'pong':
            return this.createPongMessage();
        default:
            console.log(`\u001B[33m[Protocol]\u001B[0m Cannot translate game message type: ${gameMessage.type}`);
            return null;
    }
};

// Create slither.io initial setup response
GameServer.prototype.createInitialSetupMessage = function(data) {
    const buffer = Buffer.alloc(6);
    buffer[0] = 97; // 'a' - Initial setup response
    buffer.writeUInt16BE(this.gameWidth, 1);
    buffer.writeUInt16BE(this.gameHeight, 3);
    buffer[5] = 1; // Game mode
    return buffer;
};

// Create snake position update message
GameServer.prototype.createSnakeUpdateMessage = function(snake) {
    // Ensure snake has body array
    if (!snake.body) {
        snake.body = [];
    }
    
    // This is a simplified version - real slither.io uses more complex encoding
    const buffer = Buffer.alloc(8 + snake.body.length * 4);
    buffer[0] = 71; // 'G' - Snake position update
    buffer.writeUInt16BE(snake.id, 1);
    buffer.writeUInt16BE(Math.floor(snake.x), 3);
    buffer.writeUInt16BE(Math.floor(snake.y), 5);
    buffer[7] = snake.body.length;
    
    // Add body segments
    for (let i = 0; i < snake.body.length; i++) {
        buffer.writeUInt16BE(Math.floor(snake.body[i].x), 8 + i * 4);
        buffer.writeUInt16BE(Math.floor(snake.body[i].y), 10 + i * 4);
    }
    
    return buffer;
};

// Create food update message
GameServer.prototype.createFoodUpdateMessage = function() {
    // 'f' - Food spawn
    const buffer = Buffer.alloc(this.foods.length * 5 + 1);
    buffer[0] = 102; // 'f'
    
    let offset = 1;
    for (const food of this.foods) {
        buffer.writeUInt16BE(food.x, offset);
        buffer.writeUInt16BE(food.y, offset + 2);
        buffer[offset + 4] = food.color;
        offset += 5;
    }
    
    return buffer;
};

// Create pong response
GameServer.prototype.createPongMessage = function() {
    const buffer = Buffer.alloc(1);
    buffer[0] = 112; // 'p' - Pong
    return buffer;
};

// Initialize food items
GameServer.prototype.initializeFood = function() {
    this.foods = [];
    for (let i = 0; i < 1000; i++) {
        this.foods.push({
            x: Math.floor(Math.random() * this.gameWidth),
            y: Math.floor(Math.random() * this.gameHeight),
            color: Math.floor(Math.random() * 9)
        });
    }
};

// WebSocket server creation
GameServer.prototype.start = function() {
    console.log('\u001B[31m[Game]\u001B[0m Game Server starting...');

    // CREATE WEBSOCKET SERVER HERE
    this.wss = new WebSocket.Server({
        port: this.config.port,
        path: '/slither',  // This matches what the client expects
        perMessageDeflate: false
    });

    console.log(`\u001B[31m[Game]\u001B[0m WebSocket server listening on ws://${this.config.host}:${this.config.port}/slither`);

    // HANDLE NEW CONNECTIONS
    this.wss.on('connection', (socket, req) => {
        console.log(`\u001B[32m[Client]\u001B[0m New connection from ${req.connection.remoteAddress}`);

        // Add to clients array
        const client = {
            socket: socket,
            id: this.generateClientId(),
            connected: true,
            remoteAddress: req.connection.remoteAddress,
            snake: null,
            lastPing: Date.now()
        };

        this.clients.push(client);
        console.log(`\u001B[32m[Client]\u001B[0m Total clients: ${this.clients.length}`);

        // HANDLE MESSAGES FROM CLIENT
        socket.on('message', (data) => {
            this.handleMessage(client, data);
        });

        // HANDLE CLIENT DISCONNECT
        socket.on('close', () => {
            console.log(`\u001B[33m[Client]\u001B[0m Client ${client.id} disconnected`);
            this.removeClient(client);
        });

        // HANDLE ERRORS
        socket.on('error', (error) => {
            console.log(`\u001B[31m[Error]\u001B[0m Client ${client.id} error:`, error.message);
            this.removeClient(client);
        });

        // Send initial game state to new client
        this.sendInitialGameState(client);
        
        // Start heartbeat for this client
        this.startClientHeartbeat(client);
    });

    // Start game loop
    this.startGameLoop();

    console.log('\u001B[31m[Game]\u001B[0m Game Server started successfully');
};

// Start heartbeat for client
GameServer.prototype.startClientHeartbeat = function(client) {
    client.heartbeat = setInterval(() => {
        if (client.connected && client.socket.readyState === WebSocket.OPEN) {
            // Send periodic updates to keep client alive
            const heartbeatBuffer = Buffer.alloc(1);
            heartbeatBuffer[0] = 104; // 'h' - Heartbeat
            client.socket.send(heartbeatBuffer);
        } else {
            if (client.heartbeat) {
                clearInterval(client.heartbeat);
            }
        }
    }, 1000); // Every second
};

// Game loop for updates
GameServer.prototype.startGameLoop = function() {
    setInterval(() => {
        this.updateGame();
    }, 1000 / 60); // 60 FPS for smoother updates
    
    // Separate broadcast loop - less frequent to reduce network load
    setInterval(() => {
        this.broadcastGameState();
    }, 1000 / 20); // 20 FPS broadcasts
};

// Update game state
GameServer.prototype.updateGame = function() {
    // Update snake positions with smoother movement
    for (const [id, snake] of this.snakes) {
        // More responsive movement
        snake.x += Math.cos(snake.angle) * snake.speed * 0.5;
        snake.y += Math.sin(snake.angle) * snake.speed * 0.5;
        
        // Keep within bounds with wrapping
        if (snake.x < 0) snake.x = this.gameWidth;
        if (snake.x > this.gameWidth) snake.x = 0;
        if (snake.y < 0) snake.y = this.gameHeight;
        if (snake.y > this.gameHeight) snake.y = 0;
        
        // Update body segments for smoother snake
        if (snake.body.length < snake.length) {
            snake.body.push({x: snake.x, y: snake.y});
        } else {
            // Move body segments
            for (let i = snake.body.length - 1; i > 0; i--) {
                snake.body[i].x = snake.body[i-1].x;
                snake.body[i].y = snake.body[i-1].y;
            }
            if (snake.body.length > 0) {
                snake.body[0].x = snake.x;
                snake.body[0].y = snake.y;
            }
        }
    }
};

// Broadcast game state to all clients
GameServer.prototype.broadcastGameState = function() {
    // Only broadcast if we have snakes to update
    if (this.snakes.size === 0) return;
    
    for (const [id, snake] of this.snakes) {
        const message = this.translateToSlither({
            type: 'snake_update',
            snake: snake
        });
        
        if (message) {
            this.broadcast(message);
        }
    }
};

// HANDLE MESSAGES WITH PROTOCOL TRANSLATION
GameServer.prototype.handleMessage = function(client, data) {
    try {
        console.log(`\u001B[36m[Message]\u001B[0m From ${client.id}: ${data.length} bytes`);
        
        // Translate slither.io message to game message
        const gameMessage = this.translateSlitherMessage(data);
        
        if (!gameMessage) {
            console.log(`\u001B[33m[Protocol]\u001B[0m Could not translate message from ${client.id}`);
            return;
        }

        // Handle the translated message
        switch (gameMessage.type) {
            case 'setup':
                this.handleSetupRequest(client);
                break;
            case 'angle':
                this.handleAngleUpdate(client, gameMessage.angle);
                break;
            case 'boost':
                this.handleBoostUpdate(client, gameMessage.active);
                break;
            case 'ping':
                this.handlePing(client);
                break;
            default:
                console.log(`\u001B[33m[Game]\u001B[0m Unhandled game message type: ${gameMessage.type}`);
        }

    } catch (error) {
        console.log(`\u001B[31m[Error]\u001B[0m Error handling message from ${client.id}:`, error.message);
    }
};

// Handle setup request from client
GameServer.prototype.handleSetupRequest = function(client) {
    console.log(`\u001B[36m[Game]\u001B[0m Setting up client ${client.id}`);
    
    // Create a snake for this client
    const snake = {
        id: Math.floor(Math.random() * 32767), // Ensure it fits in 16-bit signed integer
        x: Math.random() * 1000 + 500, // Smaller coordinates that fit in 16-bit
        y: Math.random() * 1000 + 500,
        angle: Math.random() * Math.PI * 2,
        speed: 5,
        body: [],
        length: 10
    };
    
    client.snake = snake;
    this.snakes.set(client.id, snake);
    
    // Send the critical initial messages that slither.io expects
    this.sendSlitherSetupSequence(client);
};

// Handle angle update from client
GameServer.prototype.handleAngleUpdate = function(client, angle) {
    if (client.snake) {
        client.snake.angle = angle;
        // More responsive - immediate update
        client.snake.x += Math.cos(angle) * 0.1;
        client.snake.y += Math.sin(angle) * 0.1;
        console.log(`\u001B[36m[Game]\u001B[0m Client ${client.id} angle: ${(angle * 180 / Math.PI).toFixed(1)}°`);
    }
};

// Handle boost update from client
GameServer.prototype.handleBoostUpdate = function(client, active) {
    if (client.snake) {
        client.snake.speed = active ? 8 : 5;
        console.log(`\u001B[36m[Game]\u001B[0m Client ${client.id} boost: ${active}`);
    }
};

// Handle ping from client
GameServer.prototype.handlePing = function(client) {
    client.lastPing = Date.now();
    
    const pongMessage = this.translateToSlither({
        type: 'pong'
    });
    
    if (pongMessage && client.socket.readyState === WebSocket.OPEN) {
        client.socket.send(pongMessage);
    }
};

// Generate unique client ID
GameServer.prototype.generateClientId = function() {
    return Math.random().toString(36).substr(2, 9);
};

// Remove client from game
GameServer.prototype.removeClient = function(client) {
    client.connected = false;
    
    // Clear heartbeat
    if (client.heartbeat) {
        clearInterval(client.heartbeat);
    }
    
    // Remove snake from game
    if (client.snake) {
        this.snakes.delete(client.id);
    }
    
    const index = this.clients.indexOf(client);
    if (index > -1) {
        this.clients.splice(index, 1);
    }
    console.log(`\u001B[32m[Client]\u001B[0m Total clients: ${this.clients.length}`);
};

// Send initial game state to new client
GameServer.prototype.sendInitialGameState = function(client) {
    try {
        console.log(`\u001B[36m[Message]\u001B[0m Sending initial state to ${client.id}`);
        
        // Client needs to send setup message first
        // Initial state will be sent in response to setup request

    } catch (error) {
        console.log(`\u001B[31m[Error]\u001B[0m Error sending initial state to ${client.id}:`, error.message);
    }
};

// Send the complete slither.io setup sequence
GameServer.prototype.sendSlitherSetupSequence = function(client) {
    const snake = client.snake;
    
    try {
        // 1. Initial setup response (opcode 'a' = 97)
        const setupBuffer = Buffer.alloc(6);
        setupBuffer[0] = 97; // 'a'
        setupBuffer.writeUInt16BE(Math.min(this.gameWidth, 65535), 1);
        setupBuffer.writeUInt16BE(Math.min(this.gameHeight, 65535), 3);
        setupBuffer[5] = 1; // Protocol version
        client.socket.send(setupBuffer);
        console.log(`\u001B[36m[Protocol]\u001B[0m Sent setup message to ${client.id}`);

        // 2. Your snake data (opcode 's' = 115) - FIXED VALUES
        const snakeBuffer = Buffer.alloc(25);
        snakeBuffer[0] = 115; // 's' - Your snake
        snakeBuffer.writeUInt16BE(Math.min(snake.id, 65535), 1); // Ensure ID fits in 16 bits
        snakeBuffer.writeUInt16BE(Math.min(Math.floor(snake.x), 65535), 3); // Ensure X fits
        snakeBuffer.writeUInt16BE(Math.min(Math.floor(snake.y), 65535), 5); // Ensure Y fits
        
        // Write angle as 16-bit integer (convert from radians)
        const angleInt = Math.floor((snake.angle / (Math.PI * 2)) * 65535);
        snakeBuffer.writeUInt16BE(angleInt, 7);
        
        snakeBuffer.writeUInt16BE(Math.min(snake.length, 65535), 9);
        snakeBuffer[11] = 0; // Name length (0 = no name shown)
        client.socket.send(snakeBuffer);
        console.log(`\u001B[36m[Protocol]\u001B[0m Sent snake data to ${client.id}`);

        // 3. Game ready (opcode 'g' = 103)
        const readyBuffer = Buffer.alloc(1);
        readyBuffer[0] = 103; // 'g' - Game ready
        client.socket.send(readyBuffer);
        console.log(`\u001B[36m[Protocol]\u001B[0m Sent game ready to ${client.id}`);

        // 4. Send some food
        setTimeout(() => {
            this.sendFoodToClient(client);
        }, 100);

    } catch (error) {
        console.log(`\u001B[31m[Error]\u001B[0m Error in setup sequence for ${client.id}:`, error.message);
    }
};

// Send food data to client
GameServer.prototype.sendFoodToClient = function(client) {
    try {
        // Send food spawn messages (opcode 'f' = 102)
        for (let i = 0; i < Math.min(50, this.foods.length); i++) {
            const food = this.foods[i];
            const foodBuffer = Buffer.alloc(6);
            foodBuffer[0] = 102; // 'f'
            foodBuffer.writeUInt16BE(food.x, 1);
            foodBuffer.writeUInt16BE(food.y, 3);
            foodBuffer[5] = food.color;
            client.socket.send(foodBuffer);
        }
        console.log(`\u001B[36m[Protocol]\u001B[0m Sent food data to ${client.id}`);
        
    } catch (error) {
        console.log(`\u001B[31m[Error]\u001B[0m Error sending food to ${client.id}:`, error.message);
    }
};

// Broadcast message to all connected clients
GameServer.prototype.broadcast = function(data) {
    this.clients.forEach(client => {
        if (client.connected && client.socket.readyState === WebSocket.OPEN) {
            client.socket.send(data);
        }
    });
};

// Load configuration
GameServer.prototype.loadConfig = function(confile) {
    try {
        // Load the contents of the config file
        var load = JSON.parse(fs.readFileSync(confile, 'utf-8'));

        // Replace all the default config's values with the loaded config's values
        for (var obj in load) {
            this.config[obj] = load[obj];
        }

        console.log('\u001B[34m[Config]\u001B[0m Loaded configuration:', this.config);

    } catch (err) {
        // No config
        console.log('\u001B[33m[Config]\u001B[0m No config file found, creating default...');

        // Create a new config
        fs.writeFileSync(confile, JSON.stringify(this.config, null, '\t'));
        console.log('\u001B[34m[Config]\u001B[0m Created default configuration file');
    }
};