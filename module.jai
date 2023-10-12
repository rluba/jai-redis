Redis_Connection :: struct {
    socket:         Socket = INVALID_SOCKET;
    verbose:        bool;
}

connect :: (redis: *Redis_Connection, hostname: string, port: u16, verbose := false) -> bool {
    redis.verbose  = verbose;

    socket, success := redis_socket_connect(hostname, port, verbose = verbose);
    redis.socket = socket;
    return success;
}

disconnect :: (redis: *Redis_Connection) {
    assert(redis.socket != INVALID_SOCKET);
    close_and_reset(*redis.socket);
}

add_redis_command :: (builder: *String_Builder, args: .. Any) -> bool {
    print_to_builder(builder, "*%\r\n", args.count);
    for arg: args {
        string_arg: string;
        if arg.type.type == {
            case .INTEGER;
                string_arg = tprint("%", arg); // @Speed
            case .STRING;
                string_arg = << cast(*string) arg.value_pointer;
            case;
                assert(false, "Unsupported argument type: %", arg.type.type);
                return false;
        }
        print_to_builder(builder, "$%\r\n%\r\n", string_arg.count, string_arg);
    }
    return true;
}

send_command :: (redis: *Redis_Connection, args: .. Any) -> bool {
    builder: String_Builder;
    defer free_buffers(*builder);
    success := add_redis_command(*builder, ..args);
    return send_command_from_builder(redis, *builder);
}

send_command_from_builder :: (redis: *Redis_Connection, builder: *String_Builder) -> bool {
    return send_builder(redis.socket, builder);
}

try_read_and_parse_responses :: (redis: *Redis_Connection, partial_data: *String_Builder) -> success: bool, [] Resp_Value {
    buffer := ensure_contiguous_space_and_return_available_range(partial_data, 128);
    success, bytes_read := read_handle(redis.socket, buffer);
    if !success {
        log_error("Could not read from Redis socket.");
        return false, .[];
    }

    if !bytes_read return true, .[];

    new_data := to_string(buffer.data, bytes_read);
    if redis.verbose {
        builder: String_Builder;
        append(*builder, new_data);
        append(*builder, "\nHex version:\n");
        for i: 0..new_data.count-1 {
            print_to_builder(*builder, "% ", formatInt(new_data[i], base = 16, minimum_digits = 2));
        }

        log("Redis said: %", builder_to_string(*builder, allocator = temp));
    }

    advance_through_ensured_space(partial_data, bytes_read);

    results: [..] Resp_Value;
    remainder := builder_to_string(partial_data, do_reset = false); // @Speed: This is very stupid…
    while true {
        result, value, remainder= := try_parse_resp_value(remainder);
        if #complete result == {
            case .ERROR;
                log_error("Could not parse Redis response.");
                return false, results;
            case .SUCCESS;
                array_add(*results, value);
            case .NOT_ENOUGH_DATA;
                // Just wait for more data…
                break;
        }
    }

    new_pending_data: String_Builder;
    new_pending_data.allocator = partial_data.allocator;
    append(*new_pending_data, remainder);
    <<partial_data = new_pending_data;
    return true, results;
}


Resp_Type :: enum u8 {
    NULL            :: #char "_";

    ERROR           :: #char "-";
    BULK_ERROR      :: #char "!";
    SIMPLE_STRING   :: #char "+";
    BULK_STRING     :: #char "$";
    VERBATIM_STRING :: #char "=";
    BIG_NUMBER      :: #char "(";

    BOOL            :: #char "#";
    INTEGER         :: #char ":";
    DOUBLE          :: #char ",";

    ARRAY           :: #char "*";
    MAP             :: #char "%";
    SET             :: #char "~";
    PUSH            :: #char ">";
}

Resp_Value :: struct {
    type: Resp_Type;
    value: union {
        _string: string;
        boolean: bool;
        integer: s64;
        double:  float64;
        array:   [] Resp_Value;
    };
}

is_null :: (value: Resp_Value) -> bool {
    return value.type == .NULL;
}

is_error :: (value: Resp_Value) -> bool {
    return value.type == .ERROR || value.type == .BULK_ERROR;
}

try_get_error :: (value: Resp_Value) -> string, success: bool {
    if value.type == {
        case .ERROR; #through;
        case .BULK_ERROR;
            return value.value._string, true;
        case;
            return "", false;
    }
}

try_get_string :: (value: Resp_Value) -> string, success: bool {
    if value.type == {
        case .SIMPLE_STRING; #through;
        case .BULK_STRING; #through;
        case .VERBATIM_STRING; #through;
        case .BIG_NUMBER;
            return value.value._string, true;
        case;
            return "", false;
    }
}

try_get_bool :: (value: Resp_Value) -> bool, success: bool {
    if value.type == {
        case .BOOL;
            return value.value.boolean, true;
        case;
            return false, false;
    }
}

try_get_s64 :: (value: Resp_Value) -> s64, success: bool {
    if value.type == {
        case .INTEGER;
            return value.value.integer, true;
        case;
            return 0, false;
    }
}

try_get_float64 :: (value: Resp_Value) -> float64, success: bool {
    if value.type == {
        case .DOUBLE;
            return value.value.double, true;
        case;
            return 0.0, false;
    }
}

try_get_values :: (value: Resp_Value) -> [] Resp_Value, success: bool {
    if value.type == {
        case .ARRAY; #through;
        case .SET; #through;
        case .PUSH; #through;
        case .MAP;
            return value.value.array, true;
        case;
            return .[], false;
    }
}

to_string :: (value: Resp_Value) -> string {
    if #complete value.type == {
        case .NULL; return sprint("%", value.type);

        case .ERROR; #through;
        case .BULK_ERROR; #through;
        case .SIMPLE_STRING; #through;
        case .BULK_STRING; #through;
        case .VERBATIM_STRING; #through;
        case .BIG_NUMBER;
            return sprint("%: %", value.type, value.value._string);

        case .BOOL;    return sprint("%: %", value.type, value.value.boolean);
        case .INTEGER; return sprint("%: %", value.type, value.value.integer);
        case .DOUBLE;  return sprint("%: %", value.type, value.value.double);

        case .ARRAY; #through;
        case .SET; #through;
        case .PUSH;
            builder: String_Builder;
            print_to_builder(*builder, "%:\n", value.type);
            for value.value.array {
                print_to_builder(*builder, "%\n", to_string(it));
            }
            return builder_to_string(*builder);

        case .MAP;
            assert(value.value.array.count % 2 == 0);
            builder: String_Builder;
            print_to_builder(*builder, "%:\n", value.type);
            for i: 0..value.value.array.count-1 {
                key := value.value.array[i];
                val := value.value.array[i + 1];
                print_to_builder(*builder, "%: %\n", to_string(key), to_string(val));
                i += 1;
            }
            return builder_to_string(*builder);
    }
}

Parse_Result :: enum {
    NOT_ENOUGH_DATA;
    ERROR;
    SUCCESS;
}

try_parse_resp_value :: (data: string) -> result: Parse_Result, Resp_Value, remainder: string {
    if !data.count return .NOT_ENOUGH_DATA, .{}, data;

    type := cast(Resp_Type) data[0];
    remainder := slice(data, 1, data.count - 1);
    if type == {
        case .NULL;
            if remainder.count < 2 return .NOT_ENOUGH_DATA, .{}, data;
            return .SUCCESS, .{type = .NULL}, slice(remainder, 2, remainder.count - 2);

        case .ERROR; #through;
        case .SIMPLE_STRING; #through;
        case .BIG_NUMBER;
            index := find_index_from_left(remainder, "\r");
            if index == -1 || index == data.count - 1 return .NOT_ENOUGH_DATA, .{}, data;
            if remainder[index + 1] != #char "\n" {
                log_error("Unexpected character after \\r: %", formatHex(remainder[index + 1]));
                return .ERROR, .{}, "";
            }

            result: Resp_Value;
            result.type = type;
            result.value._string = slice(remainder, 0, index);
            return .SUCCESS, result, slice(remainder, index + 2, remainder.count - index - 2);

        case .BULK_ERROR; #through;
        case .BULK_STRING; #through;
        case .VERBATIM_STRING;
            parse_result, length, remainder= := try_parse_resp_int(remainder);
            if parse_result != .SUCCESS return parse_result, .{}, data;

            if length + 2 > remainder.count return .NOT_ENOUGH_DATA, .{}, data;

            result: Resp_Value;
            result.type = type;
            result.value._string = slice(remainder, 0, length);
            tail := slice(remainder, length, 2);
            if tail != "\r\n" {
                log_error("Unexpected data after %: %", type, tail);
                return .ERROR, .{}, "";
            }
            return .SUCCESS, result, slice(remainder, length + 2, remainder.count - length - 2);

        case .BOOL;
            if remainder.count < 3 return .NOT_ENOUGH_DATA, .{}, data;
            tail := slice(remainder, 1, 2);
            if tail != "\r\n" {
                log_error("Unexpected data after %: %", type, tail);
                return .ERROR, .{}, "";
            }

            value: bool;
            if remainder[0] == #char "t" value = true;
            else if remainder[0] != #char "f" {
                log_error("Invalid % value: %", type, remainder);
                return .ERROR, .{}, "";
            }

            result: Resp_Value;
            result.type = type;
            result.value.boolean = value;
            return .SUCCESS, result, slice(remainder, 3, remainder.count - 3);

        case .INTEGER;
            parse_result, value, remainder= := try_parse_resp_int(remainder);
            if parse_result != .SUCCESS return parse_result, .{}, data;

            result: Resp_Value;
            result.type = type;
            result.value.integer = value;
            return .SUCCESS, result, remainder;

        case .DOUBLE;
            value, success, remainder= := string_to_float64(remainder);
            if !success {
                log_error("Could not parse %: %", type, remainder);
                return .ERROR, .{}, "";
            }

            if remainder.count < 2 return .NOT_ENOUGH_DATA, .{}, data;

            tail := slice(remainder, 0, 2);
            if tail != "\r\n" {
                log_error("Unexpected data after %: %", type, tail);
                return .ERROR, .{}, "";
            }

            result: Resp_Value;
            result.type = type;
            result.value.double = value;
            return .SUCCESS, result, slice(remainder, 2, remainder.count - 2);

        case .ARRAY; #through;
        case .SET; #through;
        case .PUSH; #through;
        case .MAP;
            parse_result, count, remainder= := try_parse_resp_int(remainder);
            if parse_result != .SUCCESS return parse_result, .{}, data;

            if type == .MAP count *= 2;

            values := NewArray(count, Resp_Value);
            for i: 0..count-1 {
                parse_result, value, remainder= := try_parse_resp_value(remainder);
                if parse_result != .SUCCESS return parse_result, .{}, data;
                values[i] = value;
            }

            result: Resp_Value;
            result.type = type;
            result.value.array = values;
            return .SUCCESS, result, remainder;

        case;
            log_error("Unknown RESP type: 0x%", formatInt(cast(int) type, base = 16));
            return .ERROR, .{}, "";
    }
}

#scope_file

// @ToDo: Allow async writes to avoid blocking on full socket buffers
send_builder :: (socket: Socket, builder: *String_Builder) -> bool {
    // @ToDo: Do this in a blocking way instead of busy-looping!
    buffer := get_base_buffer(builder);
    while buffer {
        data := get_buffer_data(buffer);
        remaining := buffer.count;
        while remaining {
            result := send(socket, data, cast(u64) remaining, 0);
            if result == -1 {
                error_code := get_last_socket_error();
                error_string := System.get_error_string(error_code);
                log_error("Could not send response: % %", error_code, error_string);
                return false;
            }
            assert(result <= remaining);
            data += result;
            remaining -= result;
        }

        buffer = buffer.next;
    }
    return true;
}


redis_socket_connect :: (hostname: string, port: u16, verbose := false) -> Socket, bool {
    hostname_c_string := to_c_string(hostname);
    defer free(hostname_c_string);

    addresses: *addrinfo;
    hints: addrinfo;
    hints.ai_family = AF_INET;
    hints.ai_socktype = .STREAM;
    hints.ai_protocol = .TCP;
    #if OS == .LINUX {
        hints.ai_flags = AI.V4MAPPED | .ADDRCONFIG;
    }
    #if OS == .MACOS {
        hints.ai_flags = AI.V4MAPPED_CFG | .ADDRCONFIG;
    }
    result := getaddrinfo(hostname_c_string, null, *hints, *addresses);
    if result != 0 {
        if verbose log_error("Could not look up \"%\": % %", hostname, result, System.get_error_string(xx result));
        return INVALID_SOCKET, false;
    }
    defer freeaddrinfo(addresses);

    address := addresses[0].ai_addr;
    address_len := addresses[0].ai_addrlen;
    address_string := "";
    defer free(address_string);
    if address.sa_family == AF_INET {
        address_v4 := cast(*sockaddr_in) address;
        address_v4.sin_port = htons(port);
        if verbose address_string = to_string(address_v4);
    } else {
        assert(address.sa_family == AF_INET6);
        address_v6 := cast(*sockaddr_in6) address;
        address_v6.sin6_port = htons(port);
        // @ToDo: to_string conversion
    }

    if verbose log("Trying to connect to %", address_string);

    sock := socket(AF_INET, .STREAM, .TCP);
    if sock == INVALID_SOCKET {
        error_code := get_last_socket_error();
        error_string := System.get_error_string(error_code);
        log_error("Couldn't create socket: % %", error_code, error_string);
        return INVALID_SOCKET, false;
    }

    success := set_keepalive(sock, true, idle_seconds_before_keepalive = 10, keepalive_interval_seconds = 5, num_keepalive_attempts = 4);
    if !success {
        return INVALID_SOCKET, false;
    }

    result = connect(sock, address, xx address_len);
    if result != 0 {
        error_code := get_last_socket_error();
        error_string := System.get_error_string(error_code);
        if verbose log_error("Could not connect to \"%:%\": error % %", hostname, port, error_code, error_string);
        close_and_reset(*sock);
        return INVALID_SOCKET, false;
    }

    #if OS == .MACOS {
        set: s32 = 1;
        result = setsockopt(sock, SOL_SOCKET, SO_NOSIGPIPE, *set, size_of(type_of(set)));
        if result == -1 {
            error_code := get_last_socket_error();
            error_string := System.get_error_string(error_code);
            log_error("Could not set socket options: error % %", error_code, error_string);
            close_and_reset(*sock);
            return INVALID_SOCKET, false;
        }
    }

    return sock, true;
}

try_parse_resp_int :: (data: string) -> result: Parse_Result, value: int, remainder: string {
    value, success, remainder := string_to_int(data);
    if !success return .ERROR, 0, "";
    if remainder.count < 2 return .NOT_ENOUGH_DATA, 0, data;

    tail := slice(remainder, 0, 2);
    if tail != "\r\n" {
        log_error("Unexpected data after integer: %", tail);
        return .ERROR, 0, "";
    }

    return .SUCCESS, value, slice(remainder, 2, remainder.count - 2);
}

formatHex :: #bake_arguments formatInt(base = 16);

read_handle :: (handle: s32, buffer: [] u8) -> success: bool, bytes_read: int {
    bytes_read := read(handle, buffer.data, cast(u64) buffer.count);
    if bytes_read < 0 {
        read_error := errno();
        if read_error == EAGAIN || read_error == EWOULDBLOCK {
            return true, 0;
        }

        return false, 0;
    }

    return true, bytes_read;
}

#import "Basic";
#import "Socket";
#import "String";
System :: #import "System";
#import "POSIX";


