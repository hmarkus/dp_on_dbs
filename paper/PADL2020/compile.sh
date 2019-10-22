#!/bin/sh
FILE="/tmp/stats$RANDOM.txt"
unamestr=$(uname)
#$i=0
python -c "print \"\n\"*1000" > /tmp/empty
if [[ "$unamestr" == 'Darwin' ]]; then
    fswatch -r -o *.tex | xargs -n1 pdflatex $1 #< $(echo "\n\n\n\n\n\n\n"))
else
    echo "0" > "$FILE"
    while :
    do
	val=`cat "$FILE"`
	valv=$val
	cmd=`find . -regex '.*.tex' | sed "s/\n/ /g"`
	for s in `stat -c %Y $cmd`
	do
		if [ "$s" -gt "$valv" ]
		then
			valv=$s
		fi
	done
	if [ "$valv" -gt "$val" ]
	then
		pdflatex $1 < /tmp/empty
		echo "$valv" > "$FILE"
	fi
	sleep 1
    done
fi
