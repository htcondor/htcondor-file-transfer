# shellcheck shell=bash disable=SC1091

# Ensure that the '.env' file exists. (The template is in Git as a separate
# file so that the user's configuration values are not tracked as changes.)

if [ ! -e "${THIS_DIR}"/.env ]; then
  printf '%s\n' "ERROR: Missing configuration file."
  printf '%s\n' ""
  printf '%s\n' "Copy '.env.template' to '.env' and update it for your setup."
  exit 1
fi

source "${THIS_DIR}"/.env.release
source "${THIS_DIR}"/.env
