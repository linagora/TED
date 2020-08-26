conf=src/config/config.ts.dist
while getopts c o; do
  echo $o;
  case $o in
    (c) conf=$OPTARG;;
    (*) usage
  esac
done

echo $conf

npm run start
