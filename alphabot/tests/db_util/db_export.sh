list=("SH_600000" "SH_600004" "SH_600006" "SH_600007" "SH_600008" "SH_600009" "SH_600010" "SH_600011" "SH_600012" "SH_600015" "SZ_000001" "SZ_000002" "SZ_000004" "SZ_000005" "SZ_000006" "SZ_000007" "SZ_000008" "SZ_000009" "SZ_000010")

for name in ${list[@]}
do
  echo $name
  mongoexport -d emquant -c $name -o $name'.json'
  # mongoimport -d emquant -c $name --drop --file $name'.json'
done
