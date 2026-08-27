# Active schemas

This directory contains the schemas implemented by the current Agent OS
runtime. CI checks schema property names against their Go wire types.

`docs/handoff/` remains the immutable architecture source preserved from the
original implementation handoff. Its schemas document that source snapshot;
they are not silently rewritten when the runtime strengthens a contract.

