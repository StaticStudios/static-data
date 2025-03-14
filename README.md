# This is not for public use
- Documentation needs to be completed
- Single point of failures need to be addressed

# Current limitations
Currently, `static-data` assumes that any column marked as an id column will not have its value changed. It should only ever be `null` when the row/data object doesn't exist. Changing this value will break things in many ways.

