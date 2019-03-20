local skynet = require "skynet"
local sc = require "skynet.socketchannel"
local socket = require "skynet.socket"
local cluster = require "skynet.cluster.core"
local ignoreret = skynet.ignoreret

local config
local channel
local session = 1

local command = {}
local waiting
local nodename = cluster.nodename()

local function read_response(sock)
	local sz = socket.header(sock:read(2))
	local msg = sock:read(sz)
	return cluster.unpackresponse(msg)	-- session, ok, data, padding
end

local function wakeup()
	if waiting then
		for _, co in ipairs(waiting) do
			skynet.wakeup(co)
		end
		waiting = nil
	end
end

local function connect(current_config)
	if current_config.host == nil then
		if not current_config.wait then
			wakeup()
		end
		return
	end

	local c = sc.channel {
			host = current_config.host,
			port = tonumber(current_config.port),
			response = read_response,
			nodelay = true,
		}
	local interval = 100
	while true do
		if pcall(c.connect, c, true) then
			if current_config == config then
				channel = c
				wakeup()
			else
				-- node change
				c:close()
			end
			return
		end
		if current_config.wait then
			skynet.sleep(interval)	-- wait and retry
			if interval < 30 * 100 then
				interval = interval + 100
			end
		else
			wakeup()
			return
		end
	end
end

local function send_request(addr, msg, sz)
	local c = channel
	-- msg is a local pointer, cluster.packrequest will free it
	local current_session = session
	local request, new_session, padding = cluster.packrequest(addr, session, msg, sz)
	session = new_session

	local tracetag = skynet.tracetag()
	if tracetag then
		if tracetag:sub(1,1) ~= "(" then
			-- add nodename
			local newtag = string.format("(%s-%s-%d)%s", nodename, node, session, tracetag)
			skynet.tracelog(tracetag, string.format("session %s", newtag))
			tracetag = newtag
		end
		skynet.tracelog(tracetag, string.format("cluster %s", node))
		c:request(cluster.packtrace(tracetag))
	end
	return c:request(request, current_session, padding)
end

local function wait()
	if config.down then
		error "Node is down"
	end
	local co = coroutine.running()
	if waiting then
		table.insert(waiting, co)
	else
		waiting = { co }
	end
	skynet.fork(connect, config)
	skynet.wait(co)
	if channel == nil then
		if config.down then
			error "Node is down"
		else
			error "Node is absent"
		end
	end
end

function command.req(...)
	if channel == nil then
		wait()
	end
	local ok, msg = pcall(send_request, ...)
	if ok then
		if type(msg) == "table" then
			skynet.ret(cluster.concat(msg))
		else
			skynet.ret(msg)
		end
	else
		skynet.error(msg)
		skynet.response()(false)
	end
end

function command.push(addr, msg, sz)
	if channel == nil then
		wait()
	end
	local request, new_session, padding = cluster.packpush(addr, session, msg, sz)
	if padding then	-- is multi push
		session = new_session
	end

	-- node_channel[node] may yield or throw error
	channel:request(request, nil, padding)
end

function command.changenode(node)
	if config and node.host == config.host and node.port == config.port and channel then
		-- only config changes
		for k,v in pairs(node) do
			config[k] = v
		end
		return
	end

	config = node
	if channel then
		local c = channel
		-- reset channel
		channel = nil
		c:close()
	end
	if waiting then
		skynet.fork(connect, config)
	end
	skynet.ret(skynet.pack(nil))
end

skynet.start(function()
	skynet.dispatch("lua", function(session , source, cmd, ...)
		local f = assert(command[cmd])
		f(...)
	end)
end)
