# Active schemas

This directory contains the schemas implemented by the current Agent OS
runtime. CI checks schema property names against their Go wire types.

Update these schemas alongside changes to the runtime contracts they describe.

Schema identifiers remain stable across directory moves. The knowledge schema's
`$id` retains its original URI; the file in this directory is the active schema.
