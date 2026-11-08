## Default Permission

Every command: connect, attach, emit (with or without ack), answer server acks, change the
auth / headers / query of the next connect, reconnect a closed client, disconnect.
Clients that Rust registered as read-only still refuse emit, ack and disconnect.

#### This default permission set includes the following:

- `allow-connect`
- `allow-disconnect`
- `allow-emit`
- `allow-emit-with-ack`
- `allow-ack`
- `allow-subscribe`
- `allow-unsubscribe`
- `allow-list`
- `allow-state`
- `allow-link-info`
- `allow-set-auth`
- `allow-set-headers`
- `allow-set-query`
- `allow-reconnect`

## Permission Table

<table>
<tr>
<th>Identifier</th>
<th>Description</th>
</tr>


<tr>
<td>

`socketio:allow-ack`

</td>
<td>

Enables the ack command without any pre-configured scope.

</td>
</tr>

<tr>
<td>

`socketio:deny-ack`

</td>
<td>

Denies the ack command without any pre-configured scope.

</td>
</tr>

<tr>
<td>

`socketio:allow-connect`

</td>
<td>

Enables the connect command without any pre-configured scope.

</td>
</tr>

<tr>
<td>

`socketio:deny-connect`

</td>
<td>

Denies the connect command without any pre-configured scope.

</td>
</tr>

<tr>
<td>

`socketio:allow-disconnect`

</td>
<td>

Enables the disconnect command without any pre-configured scope.

</td>
</tr>

<tr>
<td>

`socketio:deny-disconnect`

</td>
<td>

Denies the disconnect command without any pre-configured scope.

</td>
</tr>

<tr>
<td>

`socketio:allow-emit`

</td>
<td>

Enables the emit command without any pre-configured scope.

</td>
</tr>

<tr>
<td>

`socketio:deny-emit`

</td>
<td>

Denies the emit command without any pre-configured scope.

</td>
</tr>

<tr>
<td>

`socketio:allow-emit-with-ack`

</td>
<td>

Enables the emit_with_ack command without any pre-configured scope.

</td>
</tr>

<tr>
<td>

`socketio:deny-emit-with-ack`

</td>
<td>

Denies the emit_with_ack command without any pre-configured scope.

</td>
</tr>

<tr>
<td>

`socketio:allow-link-info`

</td>
<td>

Enables the link_info command without any pre-configured scope.

</td>
</tr>

<tr>
<td>

`socketio:deny-link-info`

</td>
<td>

Denies the link_info command without any pre-configured scope.

</td>
</tr>

<tr>
<td>

`socketio:allow-list`

</td>
<td>

Enables the list command without any pre-configured scope.

</td>
</tr>

<tr>
<td>

`socketio:deny-list`

</td>
<td>

Denies the list command without any pre-configured scope.

</td>
</tr>

<tr>
<td>

`socketio:allow-reconnect`

</td>
<td>

Enables the reconnect command without any pre-configured scope.

</td>
</tr>

<tr>
<td>

`socketio:deny-reconnect`

</td>
<td>

Denies the reconnect command without any pre-configured scope.

</td>
</tr>

<tr>
<td>

`socketio:allow-set-auth`

</td>
<td>

Enables the set_auth command without any pre-configured scope.

</td>
</tr>

<tr>
<td>

`socketio:deny-set-auth`

</td>
<td>

Denies the set_auth command without any pre-configured scope.

</td>
</tr>

<tr>
<td>

`socketio:allow-set-headers`

</td>
<td>

Enables the set_headers command without any pre-configured scope.

</td>
</tr>

<tr>
<td>

`socketio:deny-set-headers`

</td>
<td>

Denies the set_headers command without any pre-configured scope.

</td>
</tr>

<tr>
<td>

`socketio:allow-set-query`

</td>
<td>

Enables the set_query command without any pre-configured scope.

</td>
</tr>

<tr>
<td>

`socketio:deny-set-query`

</td>
<td>

Denies the set_query command without any pre-configured scope.

</td>
</tr>

<tr>
<td>

`socketio:allow-state`

</td>
<td>

Enables the state command without any pre-configured scope.

</td>
</tr>

<tr>
<td>

`socketio:deny-state`

</td>
<td>

Denies the state command without any pre-configured scope.

</td>
</tr>

<tr>
<td>

`socketio:allow-subscribe`

</td>
<td>

Enables the subscribe command without any pre-configured scope.

</td>
</tr>

<tr>
<td>

`socketio:deny-subscribe`

</td>
<td>

Denies the subscribe command without any pre-configured scope.

</td>
</tr>

<tr>
<td>

`socketio:allow-unsubscribe`

</td>
<td>

Enables the unsubscribe command without any pre-configured scope.

</td>
</tr>

<tr>
<td>

`socketio:deny-unsubscribe`

</td>
<td>

Denies the unsubscribe command without any pre-configured scope.

</td>
</tr>

<tr>
<td>

`socketio:allow-read`

</td>
<td>

Observe only: list the clients, attach to one (events and state over a channel), detach,
read its state and link info. No connect, emit, ack or disconnect.


</td>
</tr>
</table>
