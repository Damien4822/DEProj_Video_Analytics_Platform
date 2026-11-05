# Load ONE platform's credentials into the current shell. Must be sourced, not executed:
#
#     source clouds/use.sh databricks      # reads clouds/databricks/databricks.env
#     source clouds/use.sh aws             # reads clouds/aws/aws.env
#
# Switching platforms first unsets the variables the previous call loaded, so two platforms'
# credentials are never in the environment together. `source clouds/use.sh --clear` unloads.

if [[ "${BASH_SOURCE[0]}" == "$0" ]]; then
    echo "source this file instead: source clouds/use.sh <databricks|aws>" >&2
    exit 1
fi

_clouds_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

_platform="${1:-}"
_file="$_clouds_dir/$_platform/$_platform.env"
if [[ "$_platform" != "--clear" && ( -z "$_platform" || ! -f "$_file" ) ]]; then
    echo "usage: source clouds/use.sh <platform>   (expects $_clouds_dir/<platform>/<platform>.env)" >&2
    unset _clouds_dir _platform _file
    return 1   # nothing was changed: a typo does not unload the platform that is already loaded
fi

for _v in ${CLOUDS_ENV_VARS:-}; do unset "$_v"; done
unset CLOUDS_ENV_VARS CLOUDS_PLATFORM _v

if [[ "$_platform" == "--clear" ]]; then
    echo "cleared the cloud credentials from this shell"
    unset _clouds_dir _platform _file
    return 0
fi

CLOUDS_ENV_VARS="$(grep -E '^[A-Za-z_][A-Za-z0-9_]*=' "$_file" | cut -d= -f1 | tr '\n' ' ')"
set -a; source "$_file"; set +a
export CLOUDS_ENV_VARS CLOUDS_PLATFORM="$_platform"
echo "loaded $_platform: $CLOUDS_ENV_VARS"
unset _clouds_dir _platform _file
