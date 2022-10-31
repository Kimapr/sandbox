--[[
Copyright (C)2022 Kimapr <kimapr@mail.ru>

Permission is hereby granted, free of charge, to any person obtaining
a copy of this software and associated documentation files (the
"Software"), to deal in the Software without restriction, including
without limitation the rights to use, copy, modify, merge, publish,
distribute, sublicense, and/or sell copies of the Software, and to
permit persons to whom the Software is furnished to do so, subject
to the following conditions:

The above copyright notice and this permission notice shall be included
in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY
KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE
WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE
LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION
WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
--]]

local sand={}

assert(_VERSION=="Lua 5.1","only lua5.1 supported")

local crashwrap
do
	local error=error
	local pcall=pcall
	local unpack=unpack
	crashwrap=function(fn,...)
		local ret={pcall(fn,...)}
		if not ret[1] then
			error(ret[2],3)
		end
		return unpack(ret,2)
	end
end

do
	local cwrapped={}
	setmetatable(cwrapped,{__mode="k"})
	local coroutine=coroutine
	local error=error
	local function isC(fn)
		return coroutine.wrap(function()
			setfenv(0,{getfenv=getfenv})
			return getfenv(fn)
		end)()~=getfenv(fn)
	end
	local function cwrap(fn)
		if isC(fn) then return fn end
		local setfenv,getfenv=setfenv,getfenv
		local out=function(...)
			setfenv(fn,getfenv(0))
			return fn(...)
		end
		cwrapped[out]=true
		return out
	end
	local rgetfenv,rsetfenv=getfenv,setfenv
	function getfenv(fn)
		if fn and cwrapped[fn] then
			return rgetfenv(0)
		end
		return crashwrap(rgetfenv,fn)
	end
	function setfenv(fn,e)
		if fn and cwrapped[fn] then
			error("'setfenv' cannot change environment of given object",2)
		end
		return crashwrap(rsetfenv,fn,e)
	end
	local string_dump=string.dump
	function string.dump(fn)
		if cwrapped[fn] then
			error("unable to dump given function",2)
		end
		return crashwrap(string_dump,fn)
	end
	getfenv=cwrap(getfenv)
	setfenv=cwrap(setfenv)
	string.dump=cwrap(string.dump)
	sand.cwrap=cwrap
end

local cwrap=sand.cwrap

local boxes={}
local mainbox={}

local enter
local regbox
local coroid
local thrs

do
	local type,unpack,error,next,rawequal,rawset,rawget,
	      resume,yield,create,running =
		type,unpack,error,next,rawequal,rawset,rawget,
		coroutine.resume,
		coroutine.yield,
		coroutine.create,
		coroutine.running
	thrs=setmetatable({},{__mode="k"})
	local maincoro={nil}
	coroid=function()
		return running() or maincoro
	end
	thrs[coroid()]={
		envs=setmetatable({
			[mainbox]=getfenv(0)
		},{__mode="k"}),
		curbox=mainbox
	}
	local rsetfenv=setfenv
	local gstrmeta=getmetatable("")
	local gstrmetameta
	local rsetmetatable=setmetatable
	local function pp(box)
		return box==mainbox and "MAIN" or tostring(box)
	end
	function setmetatable(t,m)
		crashwrap(rsetmetatable,t,m)
		if rawequal(t,gstrmeta) then
			gstrmetameta=m
		end
		return t
	end
	function setfenv(f,e)
		if rawequal(f,0) and type(e)=="table" then
			thrs[coroid()].envs[thrs[coroid()].curbox]=e
		end
		return crashwrap(rsetfenv,f,e)
	end
	local mbox=thrs[coroid()].curbox
	local function menter(box,oldbox)
		local oldbox=mbox
		oldbox.strmeta={fields={},meta=gstrmetameta}
		for k,v in next,gstrmeta do
			oldbox.strmeta.fields[k]=v
			rawset(gstrmeta,k,nil)
		end
		for k,v in next,box.strmeta.fields do
			rawset(gstrmeta,k,v)
		end
		local oldmtf
		if gstrmetameta then
			oldmtf=rawget(gstrmetameta,"__metatable")
			rawset(gstrmetameta,"__metatable",nil)
		end
		rsetmetatable(gstrmeta,box.strmeta.meta)
		gstrmetameta=box.strmeta.meta
		if gstrmetameta then
			rawset(gstrmetameta,"__metatable",oldmtf)
		end
		mbox=box
	end
	enter=function(box)
		assert(thrs[coroid()],"current coroutine not tracked by sandbox")
		local curbox=thrs[coroid()].curbox
		if curbox==box then return curbox end
		if not thrs[coroid()].envs[box] then
			error("wacky",2)
		end
		menter(box)
		thrs[coroid()].curbox=box
		rsetfenv(0,thrs[coroid()].envs[box])
		return curbox
	end
	regbox=function(box,e)
		assert(box and type(e)=="table")
		for k,v in pairs(thrs) do
			v.envs[box]=e
		end
	end
	function coroutine.resume(coro,...)
		if coro then
			if thrs[coro] and not thrs[coroid()] then
				error("attempt to resume sandboxed coroutine from untracked coroutine",2)
			end
			if (not thrs[coro]) or (thrs[coro] and thrs[coroid()] and thrs[coro].curbox==thrs[coroid()].curbox) then
				return crashwrap(resume,coro,...)
			end
			menter(thrs[coro].curbox)
		end
		local ret={crashwrap(resume,coro,...)}
		assert(coro,"wat")
		menter(thrs[coroid()].curbox)
		return unpack(ret)
	end
	function coroutine.create(fn)
		local coro=crashwrap(create,fn)
		if not thrs[coroid()] then
			return coro
		end
		thrs[coro]={
			envs=setmetatable({},{__mode="k"}),
			curbox=thrs[coroid()].curbox
		}
		for k,v in pairs(thrs[coroid()].envs) do
			thrs[coro].envs[k]=v
		end
		return coro
	end
	local aresume,acreate=coroutine.resume,coroutine.create
	function coroutine.wrap(fn)
		local coro=acreate(fn)
		return function(...)
			local ret={aresume(coro,...)}
			if not ret[1] then
				error(ret[2],2)
			end
			return unpack(ret,2)
		end
	end
	setfenv=cwrap(setfenv)
	setmetatable=cwrap(setmetatable)
	coroutine.resume=cwrap(coroutine.resume)
	coroutine.create=cwrap(coroutine.create)
	coroutine.wrap=cwrap(coroutine.wrap)
end

local safes={
	number=true,
	bool=true,
	string=true,
	["nil"]=true
}
local function safify(v,seen)
	if safes[type(v)] then return v end
	seen=seen or {}
	if not seen[v] then
		if type(v)=="function" then
			local unpack=unpack or table.unpack
			local sand=sand
			seen[v]=cwrap(v)
		elseif type(v)=="table" then
			local t={}
			for k,vv in pairs(v) do
				t[k]=safify(vv,seen)
			end
			seen[v]=t
		else
			error("unsafe type: "..type(v))
		end
	end
	return seen[v]
end

local function wrapself(t,fn)
	return function(self,...)
		if self==t then
			return fn(...)
		end
		return fn(self,...)
	end
end

function sand.new()
	local e={}
	for k,v in ipairs{
		"_VERSION",
		"assert",
		"collectgarbage",
		"coroutine",
		"error",
		"gcinfo",
		"getfenv",
		"getmetatable",
		"ipairs",
		"load",
		"loadstring",
		"math",
		"newproxy",
		"next",
		"pairs",
		"pcall",
		"print",
		"rawequal",
		"rawget",
		"rawset",
		"select",
		"setfenv",
		"setmetatable",
		"string",
		"table",
		"tonumber",
		"tostring",
		"type",
		"unpack",
		"xpcall",
	} do
		e[v]=_G[v]
	end
	do
		local string,load,loadstring=string,load,loadstring
		e.debug={
			traceback=debug.traceback
		}
		e.io={
			write=io.write
		}
		function e.loadstring(str,...)
			if string.sub(str,1,1)=='\27' then
				return nil,"attempt to load a binary chunk"
			end
			return crashwrap(loadstring,str,...)
		end
		function e.load(fn,...)
			local str1=fn()
			local strdone=true
			if str1 and string.sub(str1,1,1)=="\27" then
				return nil,"attempt to load a binary chunk"
			end
			return crashwrap(load,function()
				if strdone then
					strdone=false
					return str1
				end
				return fn()
			end,...)
		end
	end
	e=safify(e)
	e._G=e
	local box={
		strmeta={
			fields={
				__index=e.string
			}
		}
	}
	regbox(box,e)
	local t={}
	local eload,eloadstring=e.load,e.loadstring
	function t.run(str,...)
		local L
		if type(str)=="string" then
			L=eloadstring
		elseif type(str)=="function" then
			L=eload
		end
		local ok,err=L(str,"sandbox")
		if ok then
			setfenv(ok,thrs[coroid()].envs[box])
			return t.call(ok,...)
		end
		return ok,err
	end
	local old=thrs[coroid()].curbox
	function t.call(fn,...)
		local old=enter(box)
		local ret={pcall(fn,...)}
		enter(old)
		return unpack(ret)
	end
	function t.outcall(fn,...)
		local new=enter(old)
		local ret={pcall(fn,...)}
		enter(new)
		return unpack(ret)
	end
	for k,v in pairs(t) do
		t[k]=wrapself(t,v)
	end
	return t
end

return sand
