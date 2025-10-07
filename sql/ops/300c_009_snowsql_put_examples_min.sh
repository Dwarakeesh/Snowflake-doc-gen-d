#!/bin/bash
snowsql -q "PUT file://sql/ops/*.py @~/ AUTO_COMPRESS=FALSE;"
snowsql -q "PUT file://sql/external/* @~/ AUTO_COMPRESS=FALSE;"