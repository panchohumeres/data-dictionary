#INSTRUCTIONS ON HOW TO ADD INIT SCRIPT TO CRONTAB
#FOR AUTOMATIC EXECUTION ON SCHEDULE:
#SEE: https://unix.stackexchange.com/questions/454957/cron-job-to-run-under-conda-virtual-environment

# 1. open ~/.bashrc
nano ~/.bashrc

#2. copy last section appended by anaconda (PATHS CAN VARY ACCORDING TO ANACONDA VERSION)

# >>> conda initialize >>>
# !! Contents within this block are managed by 'conda init' !!
__conda_setup="$('/home/<USER>/anaconda2/bin/conda' 'shell.bash' 'hook' 2> /dev/null)"
if [ $? -eq 0 ]; then
    eval "$__conda_setup"
else
    if [ -f "/home/<user>/anaconda2/etc/profile.d/conda.sh" ]; then
        . "/home/<user>/anaconda2/etc/profile.d/conda.sh"
    else
        export PATH="/home/<user>/anaconda2/bin:$PATH"
    fi
fi
unset __conda_setup
# <<< conda initialize <<<

#3.
crontab -e

#4.ADD THESE LINES (CHANGE "* * * * *" for schedule configuration):

SHELL=/bin/bash
BASH_ENV=/home/<user>/.bashrc_conda
* * * * * cd /home/<user>/PATH/TO/data-dictionary && conda activate conda-3-forge && ./init_data_dict.sh -m create && conda deactivate  > ./outputs/logs.txt 2>&1



