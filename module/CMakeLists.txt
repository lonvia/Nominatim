# just use the pgxs makefile
ADD_CUSTOM_TARGET(
        mod_nominatim ALL
        DEPENDS nominatim.so
)
ADD_CUSTOM_COMMAND(
        OUTPUT nominatim.so
        COMMAND PGXS=${PGXS} PG_CONFIG=${PG_CONFIG} MODSRCDIR=${CMAKE_CURRENT_SOURCE_DIR} ${CMAKE_MAKE_PROGRAM} -f ${CMAKE_CURRENT_SOURCE_DIR}/Makefile
        COMMENT "Running external makefile ${PGXS}"
        MAIN_DEPENDENCY nominatim.c
        WORKING_DIRECTORY .
)
