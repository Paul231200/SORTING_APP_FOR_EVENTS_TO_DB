
# Скрипт для ожидания доступности RabbitMQ

TIMEOUT=15
QUIET=0

echoerr() {
    if [ "$QUIET" -ne 1 ]; then echo "$@" 1>&2; fi
}

usage() {
    echo "Usage: $0 host:port [-t timeout] [-q] [-- command args]"
    echo "  -q | --quiet                        Don't output any status messages"
    echo "  -t TIMEOUT | --timeout=timeout      Timeout in seconds, zero for no timeout"
    echo "  -- COMMAND ARGS                     Execute command with args after the test finishes"
    exit 1
}

wait_for() {
    if [ "$TIMEOUT" -gt 0 ]; then
        echoerr "Waiting $TIMEOUT seconds for $HOST:$PORT"
    else
        echoerr "Waiting for $HOST:$PORT without a timeout"
    fi
    start_ts=$(date +%s)
    while :
    do
        (echo > /dev/tcp/"$HOST"/"$PORT") >/dev/null 2>&1
        result=$?
        if [ $result -eq 0 ]; then
            end_ts=$(date +%s)
            echoerr "$HOST:$PORT is available after $((end_ts - start_ts)) seconds"
            break
        fi
        sleep 1
    done
    return $result
}


while [ $# -gt 0 ]
do
    case "$1" in
        *:* )
        HOST=$(printf "%s\n" "$1"| cut -d : -f 1)
        PORT=$(printf "%s\n" "$1"| cut -d : -f 2)
        shift 1
        ;;
        -t)
        TIMEOUT="$2"
        shift 2
        ;;
        --timeout=*)
        TIMEOUT="${1#*=}"
        shift 1
        ;;
        --)
        shift
        CLI=("$@")
        break
        ;;
        *)
        usage
        ;;
    esac
done

if [ "$HOST" = "" ] || [ "$PORT" = "" ]; then
    echoerr "Error: you need to provide a host and port to test."
    usage
fi

wait_for

if [ $? -ne 0 ]; then
    exit 1
fi

if [ "${CLI[*]}" != "" ]; then
    exec "${CLI[@]}"
fi

exit 0

