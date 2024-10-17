#!/bin/bash

#SBATCH -A p32375
#SBATCH -p short
#SBATCH --nodes=2
#SBATCH --ntasks-per-node=3
#SBATCH -t 0:50:00
#SBATCH --mem=15G
#SBATCH --mail-type=FAIL
#SBATCH --mail-user=markptorres1@gmail.com
#SBATCH --job-name=test_d_job_jya0297_%j
#SBATCH --output=/projects/p32375/bluesky-research/lib/log/test_d/jya0297-%j.log

sleep 10
echo "Finished script D"
exit 0
