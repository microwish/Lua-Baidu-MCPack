--package.cpath = package.cpath .. ";/home/work/caoguoliang/lua-mcpack/lib/?.so"
package.cpath = "/home/work/caoguoliang/lua-mcpack/lib/?.so;" .. package.cpath

local mcpack = require("mcpack")

--local subarr = { subk1 = "substr1", subk2 = 2.22, subk3 = "substr3", subk4 = false, subk5 = 5.5, subk6 = "substr6" }
local subarr = { "substr1", 2.22, "substr3", false, 5.5, "substr6" }

local arr = { key1 = "string1", key2 = 2.2, key3 = "string3", key4 = true, key5 = 2147483649, key6 = "string6", key7 = subarr }

--[[
for k, v in pairs(arr) do
	print(k, v)
	if type(v) == "table" then
		for k1, v1 in pairs(v) do
			print(k1, v1)
		end
	end
end
print("\n\n")
--]]


local pack = mcpack.array2pack(arr);
--local pack = mcpack.array2pack(arr, 100000, "MC_PACK_V1")

--print(string.byte(pack, 1, string.len(pack)), "\n")
--print(string.byte(pack, 3, 3), "\n")
--for i = 1, string.len(pack) do print(string.byte(pack, i, i)) end
--print("pack len:", string.len(pack))

local arr2 = mcpack.pack2array(pack)

for k, v in pairs(arr2) do
	print(k, v)
	if type(v) == "table" then
		for k1, v1 in pairs(v) do
			print(k1, v1)
		end
	end
end
print("\n\n")
