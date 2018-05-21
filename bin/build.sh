set -e

export BUILD_USER=${BUILD_USER:-"mycat"}
export TEST_TIMEZONE=${TEST_TIMEZONE:-"UTC"}

while getopts "u:d:t:" OPT; do
    case $OPT in
        u)
            BUILD_USER=$OPTARG;;
        t)
            TEST_TIMEZONE=$OPTARG;;
    esac
done

echo -e "\033[32mTesting user ==> [${BUILD_USER}]\033[0m"
echo -e "\033[32mUsing timezone ==> [${TEST_TIMEZONE}]\033[0m"

if [ "${TEST_TIMEZONE}" == "UTC" ]; then
    echo ""
    echo -e "\033[32mTips: If you want to change timezone,use '-t' option(e.g., -t 'Asia/Shanghai').\033[0m"
    echo -e "\033[32mSupported timezone reference: https://en.wikipedia.org/wiki/List_of_tz_database_time_zones\033[0m"
    echo ""
    sleep 5
fi

echo "Building docker images..."
docker build -t mycat  --build-arg BUILD_USER="${BUILD_USER}" \
                            --build-arg TZ="${TZ}" \
                            -f Dockerfile-mycat .
                            
docker build -t mycat-web --build-arg TZ="${TZ}" \
                            -f Dockerfile-mycat-web .


echo "Starting docker compose..."
   docker-compose -f docker-compose.yaml up -d
echo "Success!"
