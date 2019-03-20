local skynet = require "skynet"
require "skynet.manager"
local cluster = require "skynet.cluster.core"

local config_name = skynet.getenv "cluster"
local node_address = {}
local node_sender = {}
local command = {}
local config = {}

local function sender_config(node)
	local n = { name = node }
	local address = node_address[node]
	if address then
		n.host, n.port = string.match(address, "([^:]+):(.*)$")
	else
		n.down = (address == false)
	end
	n.wait = not config.nowaiting
	return n
end

local function loadconfig(tmp)
	if tmp == nil then
		tmp = {}
		if config_name then
			local f = assert(io.open(config_name))
			local source = f:read "*a"
			f:close()
			assert(load(source, "@"..config_name, "t", tmp))()
		end
	end
	local change_set = {}
	local config_change
	for name,address in pairs(tmp) do
		if name:sub(1,2) == "__" then
			name = name:sub(3)
			if config[name] ~= address then
				config_change = true
			end
			config[name] = address
			skynet.error(string.format("Config %s = %s", name, address))
		else
			assert(address == false or type(address) == "string")
			if node_address[name] ~= address then
				-- address changed
				table.insert(change_set, name)
				node_address[name] = address
			end
		end
	end
	if config_change then
		for node, c in pairs(node_sender) do
			local n = sender_config(node)
			skynet.call(c, "lua", "changenode" , n)
		end
	else
		for _, node in ipairs(change_set) do
			local c = node_sender[node]
			if c then
				local n = sender_config(node)
				skynet.call(c, "lua", "changenode" , n)
			end
		end
	end
end

function command.reload(source, config)
	loadconfig(config)
	skynet.ret(skynet.pack(nil))
end

function command.listen(source, addr, port)
	local gate = skynet.newservice("gate")
	if port == nil then
		local address = assert(node_address[addr], addr .. " is down")
		addr, port = string.match(address, "([^:]+):(.*)$")
	end
	skynet.call(gate, "lua", "open", { address = addr, port = port })
	skynet.ret(skynet.pack(nil))
end

function command.sender(source, node)
	local c = node_sender[node]
	if c == nil then
		c = skynet.newservice "clustersender"
		-- double check
		if node_sender[node] then
			skynet.kill(c)
		else
			local n = sender_config(node)
			skynet.call(c, "lua", "changenode" , n)
			node_sender[node] = c
		end
	end
	skynet.ret(skynet.pack(c))
end

local proxy = {}

function command.proxy(source, node, name)
	if name == nil then
		node, name = node:match "^([^@.]+)([@.].+)"
		if name == nil then
			error ("Invalid name " .. tostring(node))
		end
	end
	local fullname = node .. "." .. name
	if proxy[fullname] == nil then
		proxy[fullname] = skynet.newservice("clusterproxy", node, name)
	end
	skynet.ret(skynet.pack(proxy[fullname]))
end

local cluster_agent = {}	-- fd:service
local register_name = {}

local function clearnamecache()
	for fd, service in pairs(cluster_agent) do
		if type(service) == "number" then
			skynet.send(service, "lua", "namechange")
		end
	end
end

function command.register(source, name, addr)
	assert(register_name[name] == nil)
	addr = addr or source
	local old_name = register_name[addr]
	if old_name then
		register_name[old_name] = nil
		clearnamecache()
	end
	register_name[addr] = name
	register_name[name] = addr
	skynet.ret(nil)
	skynet.error(string.format("Register [%s] :%08x", name, addr))
end

function command.queryname(source, name)
	skynet.ret(skynet.pack(register_name[name]))
end

function command.socket(source, subcmd, fd, msg)
	if subcmd == "open" then
		skynet.error(string.format("socket accept from %s", msg))
		-- new cluster agent
		cluster_agent[fd] = false
		local agent = skynet.newservice("clusteragent", skynet.self(), source, fd)
		local closed = cluster_agent[fd]
		cluster_agent[fd] = agent
		if closed then
			skynet.send(agent, "lua", "exit")
			cluster_agent[fd] = nil
		end
	else
		if subcmd == "close" or subcmd == "error" then
			-- close cluster agent
			local agent = cluster_agent[fd]
			if type(agent) == "boolean" then
				cluster_agent[fd] = true
			else
				skynet.send(agent, "lua", "exit")
				cluster_agent[fd] = nil
			end
		else
			skynet.error(string.format("socket %s %d %s", subcmd, fd, msg or ""))
		end
	end
end

skynet.start(function()
	loadconfig()
	skynet.dispatch("lua", function(session , source, cmd, ...)
		local f = assert(command[cmd])
		f(source, ...)
	end)
end)
