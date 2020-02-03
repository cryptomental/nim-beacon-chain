import
  algorithm, typetraits,
  stew/varints, stew/shims/[macros, tables], chronos, chronicles, peer_pool,
  libp2p/daemon/daemonapi, faststreams/output_stream, serialization,
  json_serialization/std/options, eth/p2p/p2p_protocol_dsl,
  libp2p_json_serialization, ssz

export
  daemonapi, p2pProtocol, libp2p_json_serialization, ssz

type
  Eth2Node* = ref object of RootObj
    daemon*: DaemonAPI
    peers*: Table[PeerID, Peer]
    peerpool*: PeerPool[PeerID, Peer]
    protocolStates*: seq[RootRef]

  EthereumNode = Eth2Node # needed for the definitions in p2p_backends_helpers

  Peer* = ref object
    network*: Eth2Node
    id*: PeerID
    wasDialed*: bool
    connectionState*: ConnectionState
    protocolStates*: seq[RootRef]
    maxInactivityAllowed*: Duration
    future: Future[void]
    score*: int

  ConnectionState* = enum
    None,
    Connecting,
    Connected,
    Disconnecting,
    Disconnected

  UntypedResponder = object
    peer*: Peer
    stream*: P2PStream

  Responder*[MsgType] = distinct UntypedResponder

  MessageInfo* = object
    name*: string

    # Private fields:
    thunk*: ThunkProc
    libp2pProtocol: string
    printer*: MessageContentPrinter
    nextMsgResolver*: NextMsgResolver

  ProtocolInfoObj* = object
    name*: string
    messages*: seq[MessageInfo]
    index*: int # the position of the protocol in the
                # ordered list of supported protocols

    # Private fields:
    peerStateInitializer*: PeerStateInitializer
    networkStateInitializer*: NetworkStateInitializer
    handshake*: HandshakeStep
    disconnectHandler*: DisconnectionHandler

  ProtocolInfo* = ptr ProtocolInfoObj

  PeerStateInitializer* = proc(peer: Peer): RootRef {.gcsafe.}
  NetworkStateInitializer* = proc(network: EthereumNode): RootRef {.gcsafe.}
  HandshakeStep* = proc(peer: Peer, stream: P2PStream): Future[void] {.gcsafe.}
  DisconnectionHandler* = proc(peer: Peer): Future[void] {.gcsafe.}
  ThunkProc* = proc(daemon: DaemonAPI, stream: P2PStream): Future[void] {.gcsafe.}
  MessageContentPrinter* = proc(msg: pointer): string {.gcsafe.}
  NextMsgResolver* = proc(msgData: SszReader, future: FutureBase) {.gcsafe.}

  DisconnectionReason* = enum
    ClientShutDown
    IrrelevantNetwork
    FaultOrError

  PeerDisconnected* = object of CatchableError
    reason*: DisconnectionReason

  TransmissionError* = object of CatchableError

template `$`*(peer: Peer): string = $peer.id
chronicles.formatIt(Peer): $it

# TODO: These exists only as a compatibility layer between the daemon
# APIs and the native LibP2P ones. It won't be necessary once the
# daemon is removed.
#
proc writeAllBytes(stream: P2PStream, bytes: seq[byte]) {.async.} =
  let sent = await stream.transp.write(bytes)
  if sent != bytes.len:
    raise newException(TransmissionError, "Failed to deliver msg bytes")

template readExactly(stream: P2PStream, dst: pointer, dstLen: int): untyped =
  readExactly(stream.transp, dst, dstLen)

template openStream(node: Eth2Node, peer: Peer, protocolId: string): untyped =
  openStream(node.daemon, peer.id, @[protocolId])

#
# End of compatibility layer

proc init*(T: type Peer, network: Eth2Node, id: PeerID): Peer {.gcsafe.}

proc getPeer*(node: Eth2Node, peerId: PeerID): Peer {.gcsafe.} =
  result = node.peers.getOrDefault(peerId)
  if result == nil:
    result = Peer.init(node, peerId)
    node.peers[peerId] = result

proc getPeer*(node: Eth2Node, peerInfo: PeerInfo): Peer =
  node.getPeer(peerInfo.peer)

proc peerFromStream(node: Eth2Node, stream: P2PStream): Peer {.gcsafe.} =
  node.getPeer(stream.peer)

proc disconnect*(peer: Peer, reason: DisconnectionReason,
                 notifyOtherPeer = false) {.async.} =
  # TODO: How should we notify the other peer?
  if peer.connectionState notin {Disconnecting, Disconnected}:
    peer.connectionState = Disconnecting
    await peer.network.daemon.disconnect(peer.id)
    peer.connectionState = Disconnected
    peer.network.peers.del(peer.id)
    peer.future.complete()

proc getFuture*(peer: Peer): Future[void] {.inline.} =
  ## Returns ``peer`` lifetime Future[void].
  ## Note: This procedure is used by PeerPool.
  result = peer.future

proc getKey*(peer: Peer): PeerID {.inline.} =
  ## Returns ``peer`` ident.
  ## Note: This procedure is used by PeerPool.
  result = peer.id

proc `<`*(peera, peerb: Peer): bool {.inline.} =
  ## Comparing peers by comparing their weights/scores.
  ## Note: This procedure is used by PeerPool.
  result = (peera.score < peerb.score)

proc join*(peer: Peer): Future[void] =
  ## This procedure returns ``Future[void]`` which completes only, when
  ## ``peer`` is disconnected.
  var retFuture = newFuture[void]()
  proc continuation(udata: pointer) {.gcsafe.} =
    if not(retFuture.finished()):
      retFuture.complete()
  proc cancellation(udata: pointer) {.gcsafe.} =
    peer.future.removeCallback(continuation)

  if peer.future.finished:
    retFuture.complete()
  else:
    peer.future.addCallback(continuation)
    retFuture.cancelCallback = cancellation
  return retFuture

proc safeClose(stream: P2PStream) {.async.} =
  if P2PStreamFlags.Closed notin stream.flags:
    await close(stream)

include eth/p2p/p2p_backends_helpers
include eth/p2p/p2p_tracing
include libp2p_backends_common

proc init*(T: type Eth2Node, daemon: DaemonAPI): Future[T] {.async.} =
  new result
  result.daemon = daemon
  result.daemon.userData = result
  result.peers = initTable[PeerID, Peer]()
  result.peerpool = newPeerPool[PeerID, Peer]()

  newSeq result.protocolStates, allProtocols.len
  for proto in allProtocols:
    if proto.networkStateInitializer != nil:
      result.protocolStates[proto.index] = proto.networkStateInitializer(result)

    for msg in proto.messages:
      if msg.libp2pProtocol.len > 0 and msg.thunk != nil:
        await daemon.addHandler(@[msg.libp2pProtocol], msg.thunk)

proc init*(T: type Peer, network: Eth2Node, id: PeerID): Peer =
  new result
  result.id = id
  result.network = network
  result.connectionState = Connected
  result.maxInactivityAllowed = 15.minutes # TODO: Read this from the config
  newSeq result.protocolStates, allProtocols.len
  for i in 0 ..< allProtocols.len:
    let proto = allProtocols[i]
    if proto.peerStateInitializer != nil:
      result.protocolStates[i] = proto.peerStateInitializer(result)
  result.future = newFuture[void]()

proc registerMsg(protocol: ProtocolInfo,
                 name: string,
                 thunk: ThunkProc,
                 libp2pProtocol: string,
                 printer: MessageContentPrinter) =
  protocol.messages.add MessageInfo(name: name,
                                    thunk: thunk,
                                    libp2pProtocol: libp2pProtocol,
                                    printer: printer)

proc p2pProtocolBackendImpl*(p: P2PProtocol): Backend =
  var
    Format = ident "SSZ"
    Responder = bindSym "Responder"
    DaemonAPI = bindSym "DaemonAPI"
    P2PStream = ident "P2PStream"
    OutputStream = bindSym "OutputStream"
    Peer = bindSym "Peer"
    Eth2Node = bindSym "Eth2Node"
    messagePrinter = bindSym "messagePrinter"
    milliseconds = bindSym "milliseconds"
    registerMsg = bindSym "registerMsg"
    initProtocol = bindSym "initProtocol"
    bindSymOp = bindSym "bindSym"
    errVar = ident "err"
    msgVar = ident "msg"
    msgBytesVar = ident "msgBytes"
    daemonVar = ident "daemon"
    await = ident "await"
    callUserHandler = ident "callUserHandler"

  p.useRequestIds = false
  p.useSingleRecordInlining = true

  new result

  result.PeerType = Peer
  result.NetworkType = Eth2Node
  result.registerProtocol = bindSym "registerProtocol"
  result.setEventHandlers = bindSym "setEventHandlers"
  result.SerializationFormat = Format
  result.ResponderType = Responder

  result.afterProtocolInit = proc (p: P2PProtocol) =
    p.onPeerConnected.params.add newIdentDefs(streamVar, P2PStream)

  result.implementMsg = proc (msg: Message) =
    let
      protocol = msg.protocol
      msgName = $msg.ident
      msgNameLit = newLit msgName
      MsgRecName = msg.recName

    if msg.procDef.body.kind != nnkEmpty and msg.kind == msgRequest:
      # Request procs need an extra param - the stream where the response
      # should be written:
      msg.userHandler.params.insert(2, newIdentDefs(streamVar, P2PStream))
      msg.initResponderCall.add streamVar

    ##
    ## Implemenmt Thunk
    ##
    var thunkName: NimNode

    if msg.userHandler != nil:
      thunkName = ident(msgName & "_thunk")
      let userHandlerCall = msg.genUserHandlerCall(msgVar, [peerVar, streamVar])
      msg.defineThunk quote do:
        template `callUserHandler`(`peerVar`: `Peer`,
                                   `streamVar`: `P2PStream`,
                                   `msgVar`: `MsgRecName`): untyped =
          `userHandlerCall`

        proc `thunkName`(`daemonVar`: `DaemonAPI`,
                         `streamVar`: `P2PStream`): Future[void] {.gcsafe.} =
          return handleIncomingStream(`Eth2Node`(`daemonVar`.userData), `streamVar`,
                                      `MsgRecName`, `Format`)
    else:
      thunkName = newNilLit()

    ##
    ## Implement Senders and Handshake
    ##
    if msg.kind == msgHandshake:
      macros.error "Handshake messages are not supported in LibP2P protocols"
    else:
      var sendProc = msg.createSendProc()
      implementSendProcBody sendProc

    protocol.outProcRegistrations.add(
      newCall(registerMsg,
              protocol.protocolInfoVar,
              msgNameLit,
              thunkName,
              getRequestProtoName(msg.procDef),
              newTree(nnkBracketExpr, messagePrinter, MsgRecName)))

  result.implementProtocolInit = proc (p: P2PProtocol): NimNode =
    return newCall(initProtocol, newLit(p.name), p.peerInit, p.netInit)
