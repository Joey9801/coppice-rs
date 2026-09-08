# Project stage

This project is in early-stage development. There are no external users
and no compatibility guarantees to uphold yet.

Do not add backwards-compatibility work on your own initiative: no
compat shims, deprecated-but-kept fields/APIs, migration scripts, dual
read/write paths, or version-negotiation logic, unless the user
explicitly asks for it. When a change makes something obsolete, remove
it outright rather than preserving an old path alongside the new one.

This guidance is temporary and will be revisited once the project has
real users to be compatible with.
