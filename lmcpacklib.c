/**
 * Baidu MC Pack extension for Lua
 * @version 0.1
 * @author caoguoliang@baidu.com
 */
#include <mc_pack.h>

#include <lua.h>
#include <lauxlib.h>

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <limits.h>
#include <math.h>

#define MC_PACK_DEFAULT_BYTES 100000
#define MC_PACK_MAX_BYTES 15728640
#define MC_PACK_DEFAULT_VERSION 1
#define MC_PACK_V1 "MC_PACK_V1"
#define MC_PACK_V2 "MC_PACK_V2"
#define MC_PACK_TEMP_BUF_MIN 8196

static inline int get_pack_version(const char *ver_str, size_t ver_len)
{
	if (ver_str && !strncmp(ver_str, MC_PACK_V1, ver_len))
		return 1;
	if (ver_str && !strncmp(ver_str, MC_PACK_V2, ver_len))
		return 2;
	return MC_PACK_DEFAULT_VERSION;
}

typedef enum {
	//TARGET_TYPE_WARNING = -1,
	//TARGET_TYPE_UNKNOWN = 0,
	TARGET_TYPE_INT32 = 1,
	TARGET_TYPE_UINT32 = 2,
	TARGET_TYPE_INT64 = 3,
	TARGET_TYPE_UINT64 = 4,
	TARGET_TYPE_FLOAT = 5,
	TARGET_TYPE_DOUBLE = 6,
	TARGET_TYPE_STR = 8,
	TARGET_TYPE_RAW = 9//,
	//TARGET_TYPE_ARRAY = 16,
	//TARGET_TYPE_BOOL = 32,
	//TARGET_TYPE_NULL = 64,
	//TARGET_TYPE_OBJECT = 128,
	//TARGET_TYPE_ISOARR = 256
} TARGET_TYPE;

/**
 * @reference luaconf.h
 * @TODO In Pentium machines, a naive typecast from double to int in C is extremely slow, so any alternative is worth trying
 * @FIXME it's not endian safe
 */
static TARGET_TYPE guess_number_type(lua_Number d)
{
	if (d >= 0) {
		unsigned long int uli = (unsigned long)d;
		if (uli == d) {//could be rounded
			if (uli <= (unsigned long)INT_MAX)
				return TARGET_TYPE_INT32;
			else if (uli <= (unsigned long)UINT_MAX)
				return TARGET_TYPE_UINT32;
			else if (uli <= (unsigned long)LONG_MAX)
				return TARGET_TYPE_INT64;
			else
				return TARGET_TYPE_UINT64;
		}
	} else {
		long int li = (long)d;
		if (li == d) {//could be rounded
			if (li >= (long)INT_MIN)
				return TARGET_TYPE_INT32;
			else if (li >= (long)LONG_MIN)
				return TARGET_TYPE_INT64;
		}
	}

	/* commented to compromise PHP's MC Pack extension
	//could not be rounded
	float f = (float)d;
	if (fabs(d - f) <= 0.000001)
		return TARGET_TYPE_FLOAT;
	else
		return TARGET_TYPE_DOUBLE;
	*/

	return TARGET_TYPE_DOUBLE;
}

/**
 * @return mcpack/include/mc_pack_def.h enum mc_pack_error_t
 */
static int fill_pack(lua_State *L, mc_pack_t *ppack, int key_type)
{
	char *name = NULL;
	const char *s = NULL;
	size_t l;
	int rc = MC_PE_SUCCESS;
	lua_Number n;

	switch (key_type) {
		case 3://LUA_TNUMBER in lua.h
		case 4://LUA_TSTRING in lua.h
			break;
		default:
			return MC_PE_UNKNOWN;
	}

	lua_pushnil(L);
	while (lua_next(L, -2)) {
		//key type
		if (lua_type(L, -2) != key_type)
			return MC_PE_BAD_PARAM;

		//key
		switch (key_type) {
			case 4:
				name = (char *)lua_tostring(L, -2);
				break;
			case 3:
				break;
		}

		//value
		switch (lua_type(L, -1)) {
			case LUA_TSTRING:
				s = lua_tolstring(L, -1, &l);
				if (memchr(s, '\0', l)) {
					if ((rc = mc_pack_put_raw(ppack, name, (const void *)s, l)))
						return rc;
				} else {
					if ((rc = mc_pack_put_str(ppack, name, s)))
						return rc;
				}
				break;
			case LUA_TNUMBER: {
				n = lua_tonumber(L, -1);
				switch (guess_number_type(n)) {
					case TARGET_TYPE_INT32:
						if ((rc = mc_pack_put_int32(ppack, name, (mc_int32_t)n)))
							return rc;
						break;
					case TARGET_TYPE_UINT32:
						if ((rc = mc_pack_put_uint32(ppack, name, (mc_uint32_t)n)))
							return rc;
						break;
					case TARGET_TYPE_INT64:
						if ((rc = mc_pack_put_int64(ppack, name, (mc_int64_t)n)))
							return rc;
						break;
					case TARGET_TYPE_DOUBLE:
						if ((rc = mc_pack_put_double(ppack, name, n)))
							return rc;
						break;
					case TARGET_TYPE_UINT64:
						if ((rc = mc_pack_put_uint64(ppack, name, (mc_uint64_t)n)))
							return rc;
						break;
					case TARGET_TYPE_STR:
					case TARGET_TYPE_RAW:
						break;
					case TARGET_TYPE_FLOAT:
						if ((rc = mc_pack_put_float(ppack, name, (float)n)))
							return rc;
						break;
				}
			}
				break;
			case LUA_TBOOLEAN:
				if ((rc = mc_pack_put_bool(ppack, name, (mc_bool_t)lua_toboolean(L, -1))))
					return rc;
				break;
			case LUA_TTABLE: {
				lua_pushnil(L);
				//only check the key of the first element
				lua_next(L, -2);
				mc_pack_t *psub;
				//sub key
				switch (lua_type(L, -2)) {
					case LUA_TSTRING:
						lua_pop(L, 2);
						psub = mc_pack_put_object(ppack, name);
						if ((rc = MC_PACK_PTR_ERR(psub)))
							return rc;
						if ((rc = fill_pack(L, psub, 4)))
							return rc;
						mc_pack_finish(psub);
						break;
					case LUA_TNUMBER:
						lua_pop(L, 2);
						psub = mc_pack_put_array(ppack, name);
						if ((rc = MC_PACK_PTR_ERR(psub)))
							return rc;
						if ((rc = fill_pack(L, psub, 3)))
							return rc;
						mc_pack_finish(psub);
						break;
					default:
						return MC_PE_BAD_DATA;
				}
			}
				break;
			case LUA_TNIL:
				if ((rc = mc_pack_put_null(ppack, name)))
					return rc;
				break;
			default:
				return MC_PE_BAD_TYPE;
		}

		lua_pop(L, 1);
	}

	return MC_PE_SUCCESS;
}

static int array2pack(lua_State *L)
{
	int num_args;

	if ((num_args = lua_gettop(L)) < 1)
		return luaL_error(L, "arguments required");

	char *pack_buf = NULL, *temp_buf = NULL;
	int pack_len = MC_PACK_DEFAULT_BYTES,
		temp_len = pack_len;
	const char *ver_str = NULL;
	size_t ver_len = 0;

	//arg#1
	luaL_checktype(L, 1, LUA_TTABLE);
	//check other possible args
	switch (num_args) {
		case 1:
			break;
		case 2:
			//arg#2
			switch (lua_type(L, 2)) {
				case LUA_TNUMBER:
					pack_len = (int)lua_tointeger(L, 2);
					break;
				case LUA_TSTRING:
					ver_str = lua_tolstring(L, 2, &ver_len);
					break;
				default:
					return luaL_error(L, "invalid arg#2");
			}
			lua_pop(L, 1);
			break;
		case 3:
			//arg#2
			pack_len = luaL_checkint(L, 2);
			//arg#3
			ver_str = luaL_checklstring(L, 3, &ver_len);
			lua_pop(L, 2);
			break;
		default:
			return luaL_error(L, "too many arguments");
	}
	lua_pop(L, num_args - 1);

	if (pack_len > MC_PACK_MAX_BYTES)
		return luaL_error(L, "MC Pack size exceeded the max allowed");

	if (temp_len < MC_PACK_TEMP_BUF_MIN)
		temp_len = MC_PACK_TEMP_BUF_MIN;
	if (temp_len > MC_PACK_MAX_BYTES)
		temp_len = MC_PACK_MAX_BYTES;

	int version = get_pack_version(ver_str, ver_len);

	int rc = 0;
	mc_pack_t *ppack;

	while (1) {
		if (!pack_buf) {
			if (!(pack_buf = (char *)malloc((size_t)pack_len))) {
				if (temp_buf)
					free(temp_buf);
				return luaL_error(L, "Cannot allocate %d bytes for pack", pack_len);
			}
		}
		if (!temp_buf) {
			if (!(temp_buf = (char *)malloc((size_t)temp_len))) {
				free(pack_buf);
				return luaL_error(L, "Cannot allocate %d bytes for temp buf", temp_len);
			}
		}

		ppack = mc_pack_open_w(version, pack_buf, pack_len, temp_buf, temp_len);

		if (MC_PACK_PTR_ERR(ppack) != 0) {
			free(pack_buf);
			free(temp_buf);
			return luaL_error(L, "MC Pack creation failed: %s", mc_pack_perror(MC_PACK_PTR_ERR(ppack)));
		}

		//TODO:
		//completely support tables/arrays with pure numeric index
		rc = fill_pack(L, ppack, 4);

		if (rc == MC_PE_NO_SPACE) {
			if (pack_len == MC_PACK_MAX_BYTES)
				break;
			free(pack_buf);
			pack_buf = NULL;
			pack_len *= 2;
			if (pack_len > MC_PACK_MAX_BYTES)
				pack_len = MC_PACK_MAX_BYTES;
		} else if (rc == MC_PE_NO_TEMP_SPACE) {
			if (temp_len == MC_PACK_MAX_BYTES)
				break;
			free(temp_buf);
			temp_buf = NULL;
			temp_len *= 2;
			if (temp_len > MC_PACK_MAX_BYTES)
				temp_len = MC_PACK_MAX_BYTES;
		} else {
			break;
		}
	}

	if (rc) {
		//free(pack_buf);
		//free(temp_buf);
		mc_pack_finish(ppack);
		return luaL_error(L, "MC Pack returned error: %s", mc_pack_perror(rc));
	}

	if ((rc = mc_pack_close(ppack))) {
		//free(pack_buf);
		//free(temp_buf);
		mc_pack_finish(ppack);
		return luaL_error(L, "MC Pack close failed: %s", mc_pack_perror(rc));
	}

	if ((pack_len = mc_pack_get_size(ppack)) < 0) {
		//free(pack_buf);
		//free(temp_buf);
		mc_pack_finish(ppack);
		return luaL_error(L, "MC Pack getting size failed");
	}

	lua_pushlstring(L, (const char *)pack_buf, (size_t)pack_len);

	//free(pack_buf);
	//free(temp_buf);
	mc_pack_finish(ppack);

	return 1;
}

#define GEN_TABLE_KEY(pack_type) \
	do { \
		switch (pack_type) { \
			case MC_PT_OBJ: \
				lua_pushstring(L, (const char *)sub_key); \
				break; \
			case MC_PT_ARR: \
				lua_pushinteger(L, (lua_Integer)i); \
				break; \
		} \
	} while (0)

static int fill_array(lua_State *L, const mc_pack_t *ppack, register int pack_type)
{
	switch (pack_type) {
		case MC_PT_OBJ:
		case MC_PT_ARR:
			break;
		default:
			return MC_PE_BAD_TYPE;
	}

	lua_newtable(L);

	int rc = MC_PE_SUCCESS, rc2, i;
	mc_pack_item_t item;
	const mc_pack_t *p = NULL;
	char *sub_key = NULL;

	for (i = 0, rc2 = mc_pack_first_item(ppack, &item);
			rc2 == 0;
			rc2 = mc_pack_next_item(&item, &item)) {

		switch (pack_type) {
			case MC_PT_OBJ:
				sub_key = item.key != NULL ? (char *)mc_pack_get_subkey(item.key) : NULL;
				break;
			//case MC_PT_ARR:
			default:
				i++;
				break;
		}

		switch (item.type) {
			case MC_PT_PCK:
				break;
			case MC_PT_OBJ:
				rc = mc_pack_get_pack_from_item(&item, &p);
				if (rc < MC_PE_SUCCESS)
					return rc;

				rc = fill_array(L, p, MC_PT_OBJ);
				mc_pack_finish(p);
				if (rc == MC_PE_SUCCESS) {
					GEN_TABLE_KEY(pack_type);
					lua_pushvalue(L, -2);
					lua_rawset(L, -4);
					lua_pop(L, 1);
				} else {
					//recursion error
					lua_pop(L, 1);//pop or not?
					return rc;
				}

				break;
			case MC_PT_ARR:
				rc = mc_pack_get_array_from_item(&item, &p);

				if (rc < MC_PE_SUCCESS)
					return rc;

				rc = fill_array(L, p, MC_PT_ARR);
				mc_pack_finish(p);
				if (rc == MC_PE_SUCCESS) {
					GEN_TABLE_KEY(pack_type);
					lua_pushvalue(L, -2);
					lua_rawset(L, -4);
					lua_pop(L, 1);
				} else {
					//recursion error
					lua_pop(L, 1);//pop or not?
					return rc;
				}

				break;
			case MC_IT_BIN:
				GEN_TABLE_KEY(pack_type);
				lua_pushlstring(L, (const char *)item.value, (size_t)item.value_size);
				lua_rawset(L, -3);
				break;
			case MC_IT_TXT:
				GEN_TABLE_KEY(pack_type);
				lua_pushstring(L, (const char *)item.value);
				lua_rawset(L, -3);
				break;
			case MC_IT_I32:
				GEN_TABLE_KEY(pack_type);
				lua_pushnumber(L, (lua_Number)(*((int *)item.value)));
				lua_rawset(L, -3);
				break;
			case MC_IT_U32:
				GEN_TABLE_KEY(pack_type);
				lua_pushnumber(L, (lua_Number)(*((unsigned int *)item.value)));
				lua_rawset(L, -3);
				break;
			case MC_IT_I64:
				GEN_TABLE_KEY(pack_type);
				lua_pushnumber(L, (lua_Number)(*((long *)item.value)));
				lua_rawset(L, -3);
				break;
			case MC_IT_U64:
				GEN_TABLE_KEY(pack_type);
				lua_pushnumber(L, (lua_Number)(*((unsigned long *)item.value)));
				lua_rawset(L, -3);
				break;
			case MC_IT_DOUBLE:
			case MC_IT_FLOAT:
				GEN_TABLE_KEY(pack_type);
				lua_pushnumber(L, *((lua_Number *)item.value));
				lua_rawset(L, -3);
				break;
			case MC_IT_BOOL:
				GEN_TABLE_KEY(pack_type);
				lua_pushboolean(L, *((mc_bool_t *)item.value) ? 1 : 0);
				lua_rawset(L, -3);
				break;
			case MC_IT_NULL:
				GEN_TABLE_KEY(pack_type);
				lua_pushnil(L);
				lua_rawset(L, -3);
				break;
			default:
				return MC_PE_BAD_DATA;
		}
	}
	if (rc2 != MC_PE_NOT_FOUND) {
		lua_pop(L, 1);//pop or not?
		return rc2;
	}

	return rc;
}

static int pack2array(lua_State *L)
{
	if (lua_gettop(L) != 1)
		return luaL_error(L, "1 argument required");

	luaL_checktype(L, 1, LUA_TSTRING);

	char *pack_buf, *temp_buf;
	size_t pack_len, temp_len;

	pack_buf = (char *)lua_tolstring(L, 1, &pack_len);
	lua_pop(L, 1);

	if (pack_len > MC_PACK_MAX_BYTES)
		return luaL_error(L, "MC Pack size exceeded the max allowed");

	if (pack_len < sizeof(int))
		return luaL_error(L, "MC Pack size too small");

	temp_len = MC_PACK_DEFAULT_BYTES < MC_PACK_TEMP_BUF_MIN ?
		(MC_PACK_TEMP_BUF_MIN > MC_PACK_MAX_BYTES ? MC_PACK_MAX_BYTES : MC_PACK_TEMP_BUF_MIN) : MC_PACK_DEFAULT_BYTES;

	mc_pack_t *ppack;
	int rc = MC_PE_SUCCESS;

	temp_buf = NULL;
	while (1) {
		if (!temp_buf && !(temp_buf = malloc(temp_len)))
			return luaL_error(L, "Allocating %u bytes for temp buf failed", temp_len);

		ppack = mc_pack_open_rw(pack_buf, (int)pack_len, temp_buf, (int)temp_len);

		rc = MC_PACK_PTR_ERR(ppack);
		if (rc == MC_PE_NO_TEMP_SPACE) {
			if (temp_len == MC_PACK_MAX_BYTES)
				break;
			temp_len *= 2;
			if (temp_len > MC_PACK_MAX_BYTES)
				temp_len = MC_PACK_MAX_BYTES;
		} else if (rc < MC_PE_SUCCESS) {
			free(temp_buf);
			temp_buf = NULL;
			return luaL_error(L, "MC Pack open_rw failed: %s", mc_pack_perror(rc));
		}

		rc = fill_array(L, (const mc_pack_t *)ppack, MC_PT_OBJ);

		free(temp_buf);
		temp_buf = NULL;

		if (MC_PE_NO_TEMP_SPACE == rc) {
			if (temp_len == MC_PACK_MAX_BYTES)
				break;
			temp_len *= 2;
			if (temp_len > MC_PACK_MAX_BYTES)
				temp_len = MC_PACK_MAX_BYTES;
		} else {
			break;
		}
	}

	if (rc < 0)
		return luaL_error(L, "MC Pack creating array failed");

	return 1;
}

static const struct luaL_Reg mcpack_lib[] = {
	{ "array2pack", array2pack },
	{ "pack2array", pack2array },
	{ NULL, NULL }
};

#define LUA_MCPACKLIBNAME "mcpack"

LUALIB_API int luaopen_mcpack(lua_State *L)
{
	luaL_register(L, LUA_MCPACKLIBNAME, mcpack_lib);
	return 1;
}
