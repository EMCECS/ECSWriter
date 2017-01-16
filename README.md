# ECSWriter
"write", "append", "tail", "tailbyte" working.
tailbyte is basically "tail" but only byte by byte. So it's slower overall, but doesn't wait as long for buffer to fill for small or slow tails where you want each byte right away.

./gradlew build
./gradlew shadowJar

sudo tcpdump -nnvvXS | java -jar ECSWriter.jar -a <accessKey> -b <bucket> -e <url endpoint> -s <secret key> -k <dest filename> write

sudo tcpdump -nnvvXS | java -jar ECSWriter.jar -a <accessKey> -b <bucket> -e <url endpoint> -s <secret key> -k <dest filename> append

java -jar ECSWriter.jar -a <accessKey> -b <bucket> -e <url endpoint> -s <secret key> -k <dest filename> tail

java -jar ECSWriter.jar -a <accessKey> -b <bucket> -e <url endpoint> -s <secret key> -k <dest filename> tailbyte

<url endpoint> includes any port eg http://10.1.83.51:9020
