# NSQ Throttled Producer

NSQ producers are consumer agnostic - they will keep publishing. However,
if consumers cannot handle such load, the messages get written to disk.
To prevent heavy disk usage, the throttled producer checks an upper bound
on its nsqd before publishing more.
