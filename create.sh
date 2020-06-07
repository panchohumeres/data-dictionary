#!/bin/bash

file_name="output.ipynb"
current_time=$(date "+%Y.%m.%d-%H.%M.%S")
new_fileName="${current_time}${file_name}"

papermill Data_dictionary.ipynb outputs/${new_fileName} -p mode "create"

sphinx-build -b html source build