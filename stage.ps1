param(
    [string]$Fencing = "0",
    [string]$Run = "nofence",
    [int]$PauseAt = 80,
    [int]$PauseSeconds = 40
)

[Console]::OutputEncoding = [System.Text.Encoding]::UTF8
$OutputEncoding = [System.Text.Encoding]::UTF8

$net = "zk-hw_default"
$zk = "zk1:2181,zk2:2181,zk3:2181"
$s3 = "http://host.docker.internal:9100"
$fmt = "yyyy-MM-dd HH:mm:ss.fff"

New-Item -ItemType Directory -Force -Path C:\zk-hw\outputs | Out-Null
Start-Transcript -Path "C:\zk-hw\outputs\$Run-session.txt" -Force | Out-Null

docker rm -f coord1 coord2 2>$null | Out-Null
docker run --rm --network $net -e "ZK_HOSTS=$zk" -e "S3_ENDPOINT=$s3" zkcoord python coordinator.py reset

$base = @("run", "-d", "--network", $net, "-e", "ZK_HOSTS=$zk", "-e", "S3_ENDPOINT=$s3", "-e", "FENCING=$Fencing", "-e", "RUN_NAME=$Run", "-v", "C:\zk-hw\logs:/app/logs")
docker @base --name coord1 -e COORD_ID=coord1 zkcoord python coordinator.py | Out-Null
Start-Sleep -Seconds 3
docker @base --name coord2 -e COORD_ID=coord2 zkcoord python coordinator.py | Out-Null
Write-Host "запущены coord1 и coord2, FENCING=$Fencing"

$n = 0
$deadline = (Get-Date).AddSeconds(300)
while ($n -lt $PauseAt -and (Get-Date) -lt $deadline) {
    Start-Sleep -Milliseconds 300
    $last = docker logs coord1 2>&1 | Select-String -Pattern '(\d+), \S+ \d+ \S+ \d+\s*$' | Select-Object -Last 1
    if ($last) { $n = [int]$last.Matches[0].Groups[1].Value }
}

docker pause coord1 | Out-Null
$tPause = [DateTime]::UtcNow
Write-Host ("пауза coord1 в " + $tPause.ToString($fmt) + " UTC, обработано чанков: " + $n)

Start-Sleep -Seconds $PauseSeconds
docker unpause coord1 | Out-Null
$tResume = [DateTime]::UtcNow
Write-Host ("возобновление coord1 в " + $tResume.ToString($fmt) + " UTC")

docker wait coord1 coord2 | Out-Null

Write-Host ""
Write-Host "журнал coord1:"
docker logs coord1
Write-Host ""
Write-Host "журнал coord2:"
docker logs coord2

$line = docker logs coord2 2>&1 | Select-String -Pattern '\[coord2\] лидерство получено, эпоха 2\s*$' | Select-Object -First 1
if ($line) {
    $ts = [DateTime]::ParseExact($line.Line.Substring(0, 23), $fmt, [Globalization.CultureInfo]::InvariantCulture)
    $delta = ($ts - $tPause).TotalSeconds
    Write-Host ""
    Write-Host ("смена лидера: лидерство перешло к coord2 через " + [math]::Round($delta, 1) + " с после паузы")
}

Write-Host ""
Write-Host "узел /calc/result в ZooKeeper:"
docker exec zk1 zkCli.sh -server localhost:2181 get -s /calc/result 2>$null | Select-Object -Last 16

docker rm coord1 coord2 | Out-Null
Stop-Transcript | Out-Null