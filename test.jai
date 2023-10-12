main :: () {
    success := socket_init(); // If this client ever works on Windows, we need to call this function first – because Windows…
    assert(success);

    redis: Redis_Connection;
    success = connect(*redis, "localhost", 6379, verbose = true);
    assert(success);
    defer disconnect(*redis);

    partial_data: String_Builder;
    supports_resp3 := false;
    success = send_command(*redis, "HELLO", 3); // Upgrade to RESP3, if supported
    assert(success);
    while true {
        // You might want to wait until redis.socket becomes readable here, or you’ll busy loop a lot!
        success, values := try_read_and_parse_responses(*redis, *partial_data);
        assert(success);
        if values {
            assert(values.count == 1);
            log("Hello response: %", to_string(values[0]));
            if is_error(values[0]) {
                log_error("Server does not support RESP3");
            } else {
                supports_resp3 = true;
            }
            break;
        }
    }

    // You can also send multiple commands at once
    builder: String_Builder;
    defer free_buffers(*builder);
    success &&= add_redis_command(*builder, "SELECT", 3); // Switch to DB 3
    success &&= add_redis_command(*builder, "SET", "testkey", 25);
    success &&= add_redis_command(*builder, "SUBSCRIBE", "test_channel"); // If the server doesn’t support RESP3 then all commands after this will fail
    success &&= add_redis_command(*builder, "DEL", "list");
    success &&= add_redis_command(*builder, "LPUSH", "list", "l1");
    success &&= add_redis_command(*builder, "LPUSH", "list", "l2");
    success &&= add_redis_command(*builder, "PUBLISH", "test_channel", "test_publish_arg");
    success &&= add_redis_command(*builder, "SET", "testkey2", 42);
    success &&= add_redis_command(*builder, "GET", "testkey");
    success &&= add_redis_command(*builder, "LRANGE", "list", 0, 1);
    success &&= send_command_from_builder(*redis, *builder);
    assert(success);

    responses: [..] Resp_Value;
    pushes := 0;
    got_errors := false;
    while !(responses.count == 9 && (got_errors || pushes == 2)) { // If we get errors, we might not get pushes, so let’s not wait for them
        // You might want to wait until redis.socket becomes readable here, or you’ll busy loop a lot!
        success, values := try_read_and_parse_responses(*redis, *partial_data);
        assert(success);
        for value: values {
            // Split apart pushes (which can happen at any time after a SUBSCRIBE) and responses to our commands:
            if value.type == {
                case .PUSH;
                    log("Received a push: %", to_string(value));
                    pushes += 1;
                case;
                    array_add(*responses, value);
                    if is_error(value) got_errors = true;
            }
        }
    }

    log("The responses were:");
    for responses {
        log("%: %", it_index, to_string(it));
    }
}

#import,file "module.jai";
#import "Basic";
#import "Socket";
