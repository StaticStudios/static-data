# Current limitations
Currently, `static-data` assumes that any column marked as an id column will not have its value changed. It should only ever be null when the row/daya object doesn't exist. Changing this value will break things in many ways.
