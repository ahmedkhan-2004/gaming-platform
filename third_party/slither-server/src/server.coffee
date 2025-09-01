ws = require 'ws'
url = require 'url'

snake = require './entities/snake'
food = require './entities/food'
sector = require './entities/sector'

messages = require './messages'

logger = require './utils/logger'
message = require './utils/message'
math = require './utils/math'

# Constants for better maintainability
CONSTANTS =
  DEFAULT_PORT: 3000
  DEFAULT_FOOD_AMOUNT: 200
  DEFAULT_MAX_CONNECTIONS: 1000
  MAX_MESSAGE_SIZE: 227
  SNAKE_CREATION_BYTE: 115
  PONG_BYTE: 251
  PAUSE_BYTE: 32
  SPEED_MODE_BYTE: 253
  NORMAL_MODE_BYTE: 254
  MAX_ANGLE_VALUE: 250
  
  # Movement constants - finely tuned for smooth gameplay
  TICK_INTERVAL_MS: 60        # 16.67 FPS for stable movement
  BASE_SPEED: 1.8            # Normal movement speed
  BOOST_SPEED: 3.2           # Boosted movement speed
  MAX_SPEED: 5.0             # Safety speed limit
  ANGLE_TO_RADIANS: (2 * Math.PI) / 250  # Correct angle conversion
  
  # Collision and game mechanics
  FOOD_COLLISION_BUFFER: 8
  GROWTH_DIVISOR: 3
  BROADCAST_THROTTLE: 2      # Broadcast every N ticks
  
  # World boundaries (adjust based on your game world)
  WORLD_WIDTH: 65535
  WORLD_HEIGHT: 65535
  
  # Starting position multipliers
  START_X_MULTIPLIER: 28907.6
  START_Y_MULTIPLIER: 21137.4
  START_POSITION_SCALE: 5

module.exports =
class Server
  ###
  Section: Properties
  ###
  logger: null
  server: null
  counter: 0
  clients: new Map()  # Use Map for better performance
  
  foods: []
  sectors: []
  
  # Server state tracking
  isShuttingDown: false
  tickIntervals: new Set()  # Track all intervals for cleanup

  ###
  Section: Construction
  ###
  constructor: (@port = CONSTANTS.DEFAULT_PORT) ->
    @logger = new logger(this)
    @port = parseInt(@port) or CONSTANTS.DEFAULT_PORT
    global.Server = this
    
    # Graceful shutdown handling
    process.on 'SIGTERM', @shutdown.bind(this)
    process.on 'SIGINT', @shutdown.bind(this)

  ###
  Section: Public Methods
  ###
  bind: ->
    try
      @server = new ws.Server {@port, path: '/slither'}, =>
        @logger.log @logger.level.INFO, "Server listening on port #{@port} for slither connections"
        
        # Generate initial game world
        @initializeGameWorld()
      
      @server.on 'connection', @handleConnection.bind(this)
      @server.on 'error', @handleError.bind(this)
      
      @logger.log @logger.level.INFO, "WebSocket server initialized successfully"
      
    catch error
      @logger.log @logger.level.ERROR, "Failed to bind server: #{error.message}", error
      throw error

  shutdown: ->
    return if @isShuttingDown
    @isShuttingDown = true
    
    @logger.log @logger.level.INFO, "Initiating graceful shutdown..."
    
    # Clear all intervals
    for interval in @tickIntervals
      clearInterval(interval)
    @tickIntervals.clear()
    
    # Close all client connections
    @clients.forEach (conn, id) =>
      try
        conn.close(1001, 'Server shutting down')
      catch error
        @logger.log @logger.level.DEBUG, "Error closing connection #{id}: #{error.message}"
    
    # Close server
    @server?.close =>
      @logger.log @logger.level.INFO, "Server shutdown complete"
      process.exit(0)

  ###
  Section: Private Methods
  ###
  initializeGameWorld: ->
    try
      foodAmount = global.Application?.config?['food-amount'] ? CONSTANTS.DEFAULT_FOOD_AMOUNT
      @generateFood(foodAmount)
      @generateSectors()
      @logger.log @logger.level.INFO, "Game world initialized with #{@foods.length} food items"
    catch error
      @logger.log @logger.level.ERROR, "Failed to initialize game world: #{error.message}", error

  handleConnection: (conn) ->
    return if @isShuttingDown
    
    try
      conn.binaryType = 'arraybuffer'
      
      # Connection limits and validation
      maxConnections = global.Application?.config?['max-connections'] ? CONSTANTS.DEFAULT_MAX_CONNECTIONS
      if @clients.size >= maxConnections
        @logger.log @logger.level.WARN, "Connection rejected: server full (#{@clients.size}/#{maxConnections})"
        conn.close(1013, 'Server full')
        return
      
      # Origin validation
      if not @validateOrigin(conn)
        @logger.log @logger.level.WARN, "Connection rejected: invalid origin"
        conn.close(1008, 'Invalid origin')
        return
      
      # Initialize connection
      conn.id = ++@counter
      conn.isAlive = true
      conn.lastPong = Date.now()
      
      @clients.set(conn.id, conn)
      
      # Setup connection handlers
      @setupConnectionHandlers(conn)
      
      # Send initial game state
      @sendInitialState(conn)
      
      @logger.log @logger.level.DEBUG, "New connection established: #{conn.id} (#{@clients.size} total)"
      
    catch error
      @logger.log @logger.level.ERROR, "Connection setup failed: #{error.message}", error
      conn.close(1011, 'Server error')

  validateOrigin: (conn) ->
    try
      params = url.parse(conn.upgradeReq?.url or '', true).query
      origin = conn.upgradeReq?.headers?.origin
      allowedOrigins = global.Application?.config?.origins
      
      # If no origin restrictions configured, allow all
      return true unless allowedOrigins?
      
      return allowedOrigins.indexOf(origin) > -1
      
    catch error
      @logger.log @logger.level.ERROR, "Origin validation error: #{error.message}", error
      return false

  setupConnectionHandlers: (conn) ->
    # Cleanup handler
    cleanup = =>
      @logger.log @logger.level.DEBUG, "Connection #{conn.id} closed"
      
      # Clear snake update interval
      if conn.snake?.updateInterval?
        clearInterval(conn.snake.updateInterval)
        @tickIntervals.delete(conn.snake.updateInterval)
      
      # Prevent further sends
      conn.send = -> return
      
      # Remove from clients
      @clients.delete(conn.id)
      
      # Broadcast snake removal if it existed
      if conn.snake?
        try
          @broadcast messages.snake.remove(conn.snake.id)
        catch error
          @logger.log @logger.level.DEBUG, "Failed to broadcast snake removal: #{error.message}"
    
    conn.on 'message', (data) => @handleMessage(conn, data)
    conn.on 'error', (error) => 
      @logger.log @logger.level.DEBUG, "Connection error for #{conn.id}: #{error.message}"
      cleanup()
    conn.on 'close', cleanup
    
    # Heartbeat mechanism
    conn.on 'pong', =>
      conn.isAlive = true
      conn.lastPong = Date.now()

  sendInitialState: (conn) ->
    try
      @send conn.id, messages.initial if messages.initial?
    catch error
      @logger.log @logger.level.ERROR, "Failed to send initial state to #{conn.id}: #{error.message}", error

  handleMessage: (conn, data) ->
    return unless data and data.length > 0
    return if @isShuttingDown
    
    try
      # Validate message size
      if data.length >= CONSTANTS.MAX_MESSAGE_SIZE
        @logger.log @logger.level.WARN, "Oversized message from #{conn.id}: #{data.length} bytes"
        conn.close(1009, 'Message too large')
        return
      
      if data.length is 1
        @handleControlMessage(conn, data)
      else
        @handleDataMessage(conn, data)
        
    catch error
      @logger.log @logger.level.ERROR, "Message handling error for #{conn.id}: #{error.message}", error

  handleControlMessage: (conn, data) ->
    try
      value = message.readInt8(0, data)
      
      # Ignore control messages if no snake exists (except pong)
      if not conn.snake? and value isnt CONSTANTS.PONG_BYTE
        return
      
      if value >= 0 and value <= CONSTANTS.MAX_ANGLE_VALUE
        @handleDirectionChange(conn, value)
      else if value is CONSTANTS.SPEED_MODE_BYTE
        @handleSpeedMode(conn, true)
      else if value is CONSTANTS.NORMAL_MODE_BYTE
        @handleSpeedMode(conn, false)
      else if value is CONSTANTS.PONG_BYTE
        @handlePong(conn)
      else if value is CONSTANTS.PAUSE_BYTE
        @handlePause(conn)
      else
        @logger.log @logger.level.DEBUG, "Unknown control value #{value} from #{conn.id}"
          
    catch error
      @logger.log @logger.level.ERROR, "Control message error for #{conn.id}: #{error.message}", error

  handleDirectionChange: (conn, angleValue) ->
    return unless conn.snake?
    
    # Prevent duplicate direction changes
    return if angleValue is conn.snake.direction?.angle
    
    try
      # Convert angle to radians using correct mapping
      radians = angleValue * CONSTANTS.ANGLE_TO_RADIANS
      
      # Calculate direction components (maintaining protocol compatibility)
      x = Math.cos(radians) + 1
      y = Math.sin(radians) + 1
      
      # Update snake direction
      conn.snake.direction = {
        x: x * 127,  # Protocol-compatible scaling
        y: y * 127,
        angle: angleValue,
        radians: radians  # Store for efficient movement calculation
      }
      
      @logger.log @logger.level.DEBUG, "Snake #{conn.id} direction changed to #{angleValue}"
      
    catch error
      @logger.log @logger.level.ERROR, "Direction change error for #{conn.id}: #{error.message}", error

  handleSpeedMode: (conn, boosting) ->
    return unless conn.snake?
    
    try
      conn.snake.boosting = Boolean(boosting)
      @logger.log @logger.level.DEBUG, "Snake #{conn.id} boosting: #{conn.snake.boosting}"
    catch error
      @logger.log @logger.level.ERROR, "Speed mode error for #{conn.id}: #{error.message}", error

  handlePong: (conn) ->
    try
      @send conn.id, messages.pong if messages.pong?
    catch error
      @logger.log @logger.level.ERROR, "Pong response error for #{conn.id}: #{error.message}", error

  handlePause: (conn) ->
    return unless conn.snake?
    
    try
      conn.snake.paused = not conn.snake.paused
      @logger.log @logger.level.DEBUG, "Snake #{conn.id} paused: #{conn.snake.paused}"
    catch error
      @logger.log @logger.level.ERROR, "Pause handling error for #{conn.id}: #{error.message}", error

  handleDataMessage: (conn, data) ->
    try
      return unless data.length >= 2
      
      firstByte = message.readInt8(0, data)
      
      if firstByte is CONSTANTS.SNAKE_CREATION_BYTE
        @createSnake(conn, data)
      else
        @logger.log @logger.level.DEBUG, "Unknown data message type #{firstByte} from #{conn.id}"
        
    catch error
      @logger.log @logger.level.ERROR, "Data message error for #{conn.id}: #{error.message}", error

  createSnake: (conn, data) ->
    # Prevent duplicate snake creation
    if conn.snake?
      @logger.log @logger.level.WARN, "Attempted duplicate snake creation for #{conn.id}"
      return
    
    try
      # Parse snake creation data
      skin = message.readInt8(2, data)
      name = message.readString(3, data, data.byteLength)
      
      # Validate and sanitize name
      name = @sanitizeName(name)
      
      # Generate safe starting position
      startPos = @generateStartPosition()
      
      # Create snake instance
      conn.snake = new snake(conn.id, name, startPos, skin)
      
      # Initialize snake properties
      @initializeSnake(conn.snake, startPos)
      
      # Start movement updates
      @startSnakeMovement(conn)
      
      # Broadcast new snake to all clients
      @broadcast messages.snake.build(conn.snake)
      
      # Send game state to new player
      @sendGameStateToPlayer(conn)
      
      @logger.log @logger.level.INFO, "Snake '#{name}' created for connection #{conn.id}"
      
    catch error
      @logger.log @logger.level.ERROR, "Snake creation failed for #{conn.id}: #{error.message}", error

  sanitizeName: (name) ->
    try
      # Basic sanitization
      sanitized = String(name or 'Anonymous')
        .trim()
        .substring(0, 20)  # Limit length
        .replace(/[<>]/g, '')  # Remove potential HTML
      
      return sanitized or 'Anonymous'
      
    catch error
      @logger.log @logger.level.ERROR, "Name sanitization error: #{error.message}", error
      return 'Anonymous'

  generateStartPosition: ->
    try
      # Generate position with some randomness but avoid edges
      margin = 1000
      x = (CONSTANTS.START_X_MULTIPLIER + Math.random() * 2000 - 1000) * CONSTANTS.START_POSITION_SCALE
      y = (CONSTANTS.START_Y_MULTIPLIER + Math.random() * 2000 - 1000) * CONSTANTS.START_POSITION_SCALE
      
      # Ensure within world bounds
      x = Math.max(margin, Math.min(CONSTANTS.WORLD_WIDTH - margin, x))
      y = Math.max(margin, Math.min(CONSTANTS.WORLD_HEIGHT - margin, y))
      
      return {x, y}
      
    catch error
      @logger.log @logger.level.ERROR, "Start position generation error: #{error.message}", error
      return {
        x: CONSTANTS.START_X_MULTIPLIER * CONSTANTS.START_POSITION_SCALE,
        y: CONSTANTS.START_Y_MULTIPLIER * CONSTANTS.START_POSITION_SCALE
      }

  initializeSnake: (snake, startPos) ->
    try
      # Initialize direction
      snake.direction ?= {x: 0, y: 0, angle: 0, radians: 0}
      
      # Initialize state
      snake.paused = false
      snake.boosting = false
      snake.updateCounter = 0
      
      # Initialize position tracking
      snake.floatX = startPos.x
      snake.floatY = startPos.y
      
      # Ensure body position is set
      if snake.body?
        snake.body.x = startPos.x
        snake.body.y = startPos.y
      
      # Initialize segments and growth
      snake.segments ?= [{x: startPos.x, y: startPos.y}]
      snake.pendingGrowth ?= 0
      
      # Initialize last update time for consistent timing
      snake.lastUpdate = Date.now()
      
    catch error
      @logger.log @logger.level.ERROR, "Snake initialization error: #{error.message}", error

  startSnakeMovement: (conn) ->
    return unless conn.snake?
    
    try
      updateInterval = setInterval =>
        @updateSnakePosition(conn)
      , CONSTANTS.TICK_INTERVAL_MS
      
      conn.snake.updateInterval = updateInterval
      @tickIntervals.add(updateInterval)
      
    catch error
      @logger.log @logger.level.ERROR, "Movement start error for #{conn.id}: #{error.message}", error

  updateSnakePosition: (conn) ->
    return unless conn.snake? and conn.snake.body? and not @isShuttingDown
    return if conn.snake.paused
    
    try
      # Calculate time delta for consistent movement
      currentTime = Date.now()
      deltaTime = currentTime - (conn.snake.lastUpdate or currentTime)
      conn.snake.lastUpdate = currentTime
      
      # Clamp delta to prevent huge jumps
      deltaTime = Math.min(deltaTime, CONSTANTS.TICK_INTERVAL_MS * 2)
      timeFactor = deltaTime / CONSTANTS.TICK_INTERVAL_MS
      
      # Get movement direction
      radians = conn.snake.direction?.radians ? 0
      
      # Calculate speed based on boost state
      baseSpeed = if conn.snake.boosting then CONSTANTS.BOOST_SPEED else CONSTANTS.BASE_SPEED
      speed = Math.min(baseSpeed, CONSTANTS.MAX_SPEED)
      
      # Apply time-based movement
      actualSpeed = speed * timeFactor
      
      # Calculate movement delta
      dx = Math.cos(radians) * actualSpeed
      dy = Math.sin(radians) * actualSpeed
      
      # Update float positions
      conn.snake.floatX += dx
      conn.snake.floatY += dy
      
      # Apply world bounds
      @applyWorldBounds(conn.snake)
      
      # Update integer body positions
      conn.snake.body.x = Math.round(conn.snake.floatX)
      conn.snake.body.y = Math.round(conn.snake.floatY)
      
      # Check collisions
      @checkCollisions(conn)
      
      # Broadcast updates (throttled)
      @broadcastSnakeUpdate(conn)
      
    catch error
      @logger.log @logger.level.ERROR, "Position update error for #{conn.id}: #{error.message}", error

  applyWorldBounds: (snake) ->
    try
      # Keep snake within world bounds
      snake.floatX = Math.max(0, Math.min(CONSTANTS.WORLD_WIDTH, snake.floatX))
      snake.floatY = Math.max(0, Math.min(CONSTANTS.WORLD_HEIGHT, snake.floatY))
    catch error
      @logger.log @logger.level.ERROR, "World bounds error: #{error.message}", error

  checkCollisions: (conn) ->
    return unless conn.snake?
    
    try
      # Check food collisions
      @checkFoodCollisions(conn)
      
      # TODO: Add snake-to-snake collision detection
      # TODO: Add boundary collision detection with death
      
    catch error
      @logger.log @logger.level.ERROR, "Collision check error for #{conn.id}: #{error.message}", error

  checkFoodCollisions: (conn) ->
    return unless conn.snake? and @foods.length > 0
    
    try
      snakeX = conn.snake.floatX
      snakeY = conn.snake.floatY
      
      # Check each food item (iterate backwards for safe removal)
      for i in [@foods.length - 1..0]
        foodObj = @foods[i]
        continue unless foodObj?.position?
        
        # Calculate distance
        dx = snakeX - foodObj.position.x
        dy = snakeY - foodObj.position.y
        distance = Math.sqrt(dx * dx + dy * dy)
        
        # Check collision
        collisionRadius = (foodObj.size or 8) + CONSTANTS.FOOD_COLLISION_BUFFER
        if distance < collisionRadius
          @handleFoodConsumption(conn, foodObj, i)
          break  # Only eat one food per update
      
    catch error
      @logger.log @logger.level.ERROR, "Food collision error for #{conn.id}: #{error.message}", error

  handleFoodConsumption: (conn, foodObj, foodIndex) ->
    try
      # Broadcast eat event
      @broadcast messages.eat.build(foodObj.id, foodObj.size)
      
      # Remove food from server
      @foods.splice(foodIndex, 1)
      
      # Calculate growth
      growthAmount = Math.max(1, Math.round((foodObj.size or 8) / CONSTANTS.GROWTH_DIVISOR))
      conn.snake.pendingGrowth = (conn.snake.pendingGrowth or 0) + growthAmount
      
      # Add segments at tail position
      @addSnakeSegments(conn.snake, growthAmount)
      
      # Respawn food to maintain density
      @respawnFood(1)
      
      @logger.log @logger.level.DEBUG, "Snake #{conn.id} ate food #{foodObj.id}, growth: #{growthAmount}"
      
    catch error
      @logger.log @logger.level.ERROR, "Food consumption error for #{conn.id}: #{error.message}", error

  addSnakeSegments: (snake, count) ->
    try
      return unless snake.segments? and count > 0
      
      # Get tail position
      tail = snake.segments[snake.segments.length - 1] or {x: snake.body.x, y: snake.body.y}
      
      # Add segments at tail position
      for i in [0...count]
        snake.segments.push {x: tail.x, y: tail.y}
        
    catch error
      @logger.log @logger.level.ERROR, "Segment addition error: #{error.message}", error

  broadcastSnakeUpdate: (conn) ->
    return unless conn.snake?
    
    try
      conn.snake.updateCounter = (conn.snake.updateCounter or 0) + 1
      
      # Throttle broadcasts to reduce network load
      if conn.snake.updateCounter % CONSTANTS.BROADCAST_THROTTLE is 0
        @broadcast messages.direction.build(conn.snake.id, conn.snake.direction)
        @broadcast messages.movement.build(conn.snake.id, conn.snake.direction.x, conn.snake.direction.y)
        
    catch error
      @logger.log @logger.level.ERROR, "Broadcast update error for #{conn.id}: #{error.message}", error

  sendGameStateToPlayer: (conn) ->
    try
      # Send existing snakes
      @spawnSnakes(conn.id)
      
      # Send food
      @send conn.id, messages.food.build(@foods)
      
      # Send UI updates
      @sendUIUpdates(conn.id)
      
    catch error
      @logger.log @logger.level.ERROR, "Game state send error for #{conn.id}: #{error.message}", error

  sendUIUpdates: (connId) ->
    try
      clientsArray = Array.from(@clients.values()).filter(c => c.snake?)
      
      @send connId, messages.leaderboard.build(clientsArray, 1, clientsArray)
      @send connId, messages.highscore.build('Server', 'Welcome to the game!')
      @send connId, messages.minimap.build(@foods)
      
    catch error
      @logger.log @logger.level.ERROR, "UI update error for #{connId}: #{error.message}", error

  handleError: (error) ->
    switch error.code
      when 'EADDRINUSE'
        @logger.log @logger.level.ERROR, "Port #{@port} is already in use", error
      when 'EACCES'
        @logger.log @logger.level.ERROR, "Permission denied on port #{@port}", error
      else
        @logger.log @logger.level.ERROR, "Server error: #{error.message}", error

  generateFood: (amount) ->
    try
      amount = parseInt(amount) or CONSTANTS.DEFAULT_FOOD_AMOUNT
      @foods = []  # Clear existing food
      
      config = global.Application?.config or {}
      gameRadius = config['game-radius'] or CONSTANTS.WORLD_WIDTH
      colorCount = config['food-colors'] or 24
      sizeRange = config['food-size'] or [5, 12]
      
      for i in [0...amount]
        x = math.randomInt(0, CONSTANTS.WORLD_WIDTH)
        y = math.randomInt(0, CONSTANTS.WORLD_HEIGHT)
        id = x * gameRadius * 3 + y
        color = math.randomInt(0, colorCount)
        size = math.randomInt(sizeRange[0], sizeRange[1])
        
        @foods.push(new food(id, {x, y}, size, color))
        
      @logger.log @logger.level.INFO, "Generated #{@foods.length} food items"
      
    catch error
      @logger.log @logger.level.ERROR, "Food generation error: #{error.message}", error

  respawnFood: (count = 1) ->
    try
      config = global.Application?.config or {}
      gameRadius = config['game-radius'] or CONSTANTS.WORLD_WIDTH
      colorCount = config['food-colors'] or 24
      sizeRange = config['food-size'] or [5, 12]
      
      for i in [0...count]
        x = math.randomInt(0, CONSTANTS.WORLD_WIDTH)
        y = math.randomInt(0, CONSTANTS.WORLD_HEIGHT)
        id = x * gameRadius * 3 + y + Date.now()  # Ensure unique ID
        color = math.randomInt(0, colorCount)
        size = math.randomInt(sizeRange[0], sizeRange[1])
        
        newFood = new food(id, {x, y}, size, color)
        @foods.push(newFood)
        
        # Broadcast new food to all clients
        @broadcast messages.food.spawn(newFood)
        
    catch error
      @logger.log @logger.level.ERROR, "Food respawn error: #{error.message}", error

  generateSectors: ->
    try
      config = global.Application?.config or {}
      gameRadius = config['game-radius'] or CONSTANTS.WORLD_WIDTH
      sectorSize = config['sector-size'] or 500
      
      sectorsAmount = Math.ceil(gameRadius / sectorSize)
      
      @sectors = []
      for i in [0...sectorsAmount]
        @sectors.push(new sector(i))
        
      @logger.log @logger.level.INFO, "Generated #{@sectors.length} sectors"
      
    catch error
      @logger.log @logger.level.ERROR, "Sector generation error: #{error.message}", error

  spawnSnakes: (excludeId) ->
    try
      @clients.forEach (client, id) =>
        if id isnt excludeId and client.snake?
          @send excludeId, messages.snake.build(client.snake)
    catch error
      @logger.log @logger.level.ERROR, "Snake spawning error: #{error.message}", error

  send: (id, data) ->
    return unless data
    
    try
      client = @clients.get(id)
      if client? and typeof client.send is 'function'
        client.send data, {binary: true}
        return true
      return false
      
    catch error
      @logger.log @logger.level.ERROR, "Send error to #{id}: #{error.message}", error
      # Remove dead connection
      @clients.delete(id)
      return false

  broadcast: (data) ->
    return unless data
    
    sentCount = 0
    errorCount = 0
    
    @clients.forEach (client, id) =>
      try
        if client? and typeof client.send is 'function'
          client.send data, {binary: true}
          sentCount++
      catch error
        errorCount++
        # Remove dead connection
        @clients.delete(id)
    
    if errorCount > 0
      @logger.log @logger.level.DEBUG, "Broadcast: #{sentCount} sent, #{errorCount} errors"

  close: ->
    @shutdown()