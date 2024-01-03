#! /bin/bash

set -e
set -u
set -o pipefail

# This script is used to update the help messages in the docs.

SCRIPT_DIR="$(dirname "$(readlink -f "$0")")"
INCLUDE_DIR="$SCRIPT_DIR/include"

# set PGCOPYDB if not already set
if [ -z "${PGCOPYDB:-}" ]; then
    PGCOPYDB="${SCRIPT_DIR}/../src/bin/pgcopydb/pgcopydb"
fi

echo "Updating documentation templates using binary ${PGCOPYDB}"

# Given a single command, print the help text, and wrap the output in rst style
# code block format in a file under include directory
function print_help_to_file() {


    # expand all positional parameters and trim whitespace at the end
    local cmd
    cmd=$(echo "$*" | sed -E "s/^ +//")

    # Replace spaces in the command name with dashes to generate the file name.
    #
    # We print the output of
    #   `pgcopydb --help` to `pgcopydb.rst`
    #   `pgcopydb compare --help` to `compare.rst`
    #   `pgcopydb compare data --help` to `compare-data.rst` etc.
    #
    #  One exception is the `pgcopydb help` command. We print the output of
    #  `pgcopydb help` to `help.rst` instead of supplying a `--help` argument to
    #  the command.
    local file_path
    local help_cmd
    if [ "${cmd}" = "" ]; then
        help_cmd="${PGCOPYDB} --help 2>&1"
        file_path="${INCLUDE_DIR}/pgcopydb.rst"
    elif [ "${cmd}" = "help" ]; then
        help_cmd="${PGCOPYDB} help 2>&1"
        file_path="${INCLUDE_DIR}/help.rst"
    else
        help_cmd="${PGCOPYDB} ${cmd} --help 2>&1"
        file_path="${INCLUDE_DIR}/${cmd// /-}.rst"
    fi

    # Generate help text by running the command, removing the line with version
    # information and adding 3 spaces at the beginning of each line
    local help_text
    help_text="$( eval "${help_cmd}"  |
        sed -e '/.*Running pgcopydb version.*/d' -e 's/^/   /'
    )"


    # Wrao the help text in a rst code block and print to file
    {
        echo "::"
        echo
        echo "${help_text}"
    } >"${file_path}"
}

# Parse the output of `pgcopydb help` and call print_help_to_file for each command
function parse_help_output() {

    local cmd=""
    local subcmd=""
    # Loop over all the lines of the help text, parse commands and subcommands,
    # and call print_help_to_file for each command.
    #
    # Currently the output of `pgcopydb help` starts with:
    #
    #   pgcopydb
    #     clone     Clone an entire database from source to target
    #     fork      Clone an entire database from source to target
    #     follow    Replay changes from the source database to the target database
    #     ...
    #     ...
    #     ping      Attempt to connect to the source and target instances
    #     help      Print help message
    #     version   Print pgcopydb version
    #
    #   pgcopydb compare
    #     schema  Compare source and target schema
    #     data    Compare source and target data
    #
    # We parse these lines one by one, and store portions of the commands in
    # variables cmd and subcmd. For example, for the line that corresponds to
    # `pgcopydb compare schema`, we set cmd to `compare` and subcmd to `schema`
    while read -r l; do
        subcmd=""
        # Parse first section of the help text:
        #   pgcopydb
        if [[ ${l} =~ ^pgcopydb$ ]]; then
            cmd=""

        # Parse other section headers of the help text that contain `pgcopydb <cmd>`
        #
        # For example:
        #   pgcopydb compare
        #   pgcopydb copy
        #   pgcopydb dump
        #
        # These commands should already be printed in an earlier section.
        # Therefore we store the command name in a variable and move on to the
        # next line for parsing subcommands
        elif [[ ${l} =~ ^pgcopydb\ (.+) ]]; then
            cmd="${BASH_REMATCH[1]}"
            continue;

        # Parse subcommands that are followed by a section header. For example,
        # there are the subcommands under pgcopydb strem sentinel section:
        #     create  Create the sentinel table on the source database
        #     drop    Drop the sentinel table on the source database
        #     get     Get the sentinel table values on the source database
        #   + set     Maintain a sentinel table on the source database
        #
        # Here we have an optional + character followed by the subcommand name.
        # The subcommand may contain lowercase alphabetical characters or dashes
        # (e.g. table-parts).
        elif [[ ${l} =~ ^(\+ )?([a-z-]+) ]]; then
            subcmd="${BASH_REMATCH[2]}"

        # Skip all other lines that does not match. (e.g. empty lines)
        else
            continue
        fi

        # print the help message for the subcommand to file
        print_help_to_file "${cmd} ${subcmd}"

    done < <(${PGCOPYDB} help 2>&1)
}

# Delete all the existing help files and recreate them
rm -f "${INCLUDE_DIR}/*"
parse_help_output

# Remove the help messages for the commands that are not covered in docs
rm -rf "${INCLUDE_DIR}/fork.rst" \
       "${INCLUDE_DIR}/version.rst"
