#!/bin/sh

genavro(){
	export ENV_SCHEMA_FILENAME=./sample.d/sample.avsc

	cat ./sample.d/sample1.jsonl |
		json2avrows |
		cat > ./sample.d/sample1.avro

	cat ./sample.d/sample2.jsonl |
		json2avrows |
		cat > ./sample.d/sample2.avro

}

#genavro

export ENV_TARGET_COL_NAME=name

export ENV_TARGET_COL_VALUE=fuji
export ENV_TARGET_COL_VALUE=takao
export ENV_TARGET_COL_VALUE=tokio
export ENV_TARGET_COL_VALUE=tokyo
export ENV_TARGET_COL_VALUE=run
export ENV_TARGET_COL_VALUE=sky

ls sample.d/*.avro |
	./afindstring
