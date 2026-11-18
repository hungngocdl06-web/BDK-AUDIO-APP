import 'dart:async';
import 'dart:convert';
import 'dart:math' as math;
import 'dart:typed_data';
import 'package:universal_ble/universal_ble.dart';
import '../protocol/speaker_protocol.dart';
import 'ble_transport.dart';
export 'ble_transport.dart' show BleTransportException;

const _kWebBleService = '4fafc201-1fb5-459e-8fcc-c5c9c331914b';
const _kWebBleTx      = 'beb5483e-36e1-4688-b7f5-ea07361b26a8';
const _kWebBleRx      = 'beb5483f-36e1-4688-b7f5-ea07361b26a8';

class UniversalBleTransport implements BleTransport {
  UniversalBleTransport({this.timeout = const Duration(seconds: 10)}) {
    _scanSub = UniversalBle.scanStream.listen(
      (device) {
        if (!_scanController.isClosed) {
          _scanController.add(DiscoveredSpeaker(
            id: device.deviceId,
            name: device.rawName ?? device.name ?? 'Unnamed speaker',
            rssi: device.rssi,
            services: List.unmodifiable(device.services),
          ));
        }
      },
      onError: (Object error, StackTrace stackTrace) {
        if (!_scanController.isClosed) {
          _scanController.addError(error, stackTrace);
        }
      },
    );
  }
  final Duration timeout;
  final _links = <_UniversalBleLink>{};
  final _devices = <String, _DeviceConnectionOwner>{};
  final _scanController = StreamController<DiscoveredSpeaker>.broadcast();
  StreamSubscription<BleDevice>? _scanSub;
  bool _disposed = false;

  @override
  Stream<DiscoveredSpeaker> get scanResults => _scanController.stream;

  void _checkAlive() {
    if (_disposed) {
      throw const BleTransportException('Bluetooth transport is closed.');
    }
  }

  Future<void> _ensureAvailable() async {
    _checkAlive();
    if (!await UniversalBle.hasPermissions().timeout(timeout)) {
      await UniversalBle.requestPermissions().timeout(timeout);
      if (!await UniversalBle.hasPermissions().timeout(timeout)) {
        throw const BleTransportException(
          'Allow Bluetooth / Nearby devices permission in system settings, then try again.',
        );
      }
    }
    final availability = await UniversalBle.getBluetoothAvailabilityState()
        .timeout(timeout);
    if (availability != AvailabilityState.poweredOn) {
      throw BleTransportException(switch (availability) {
        AvailabilityState.poweredOff =>
          'Turn on Bluetooth in system settings, then try again.',
        AvailabilityState.unauthorized =>
          'Allow Bluetooth access in system settings, then try again.',
        AvailabilityState.unsupported =>
          'Bluetooth Low Energy is unavailable on this device or adapter.',
        _ =>
          'Bluetooth is ${availability.name}; wait for the adapter and try again.',
      });
    }
  }

  @override
  Future<void> startScan() async {
    await _ensureAvailable();
    try {
      final systemDevices = await UniversalBle.getSystemDevices(
        withServices: [SpeakerProtocol.service, _kWebBleService],
      );
      for (final device in systemDevices) {
        if (!_scanController.isClosed) {
          _scanController.add(DiscoveredSpeaker(
            id: device.deviceId,
            name: device.rawName ?? device.name ?? 'Unnamed speaker',
            rssi: -50,
            services: List.unmodifiable(device.services),
          ));
        }
      }
    } catch (_) {}

    await UniversalBle.startScan(
      platformConfig: PlatformConfig(
        android: AndroidOptions(
          scanMode: AndroidScanMode.lowLatency,
          reportDelayMillis: 0,
        ),
      ),
    ).timeout(timeout);
  }

  @override
  Future<void> stopScan() async {
    _checkAlive();
    await UniversalBle.stopScan().timeout(timeout);
  }

  @override
  Future<BleLink> connect(String id) {
    _checkAlive();
    final key = UniversalBle.canonicalDeviceId(id);
    final owner = _devices.putIfAbsent(key, _DeviceConnectionOwner.new);
    return owner.run(() => _connectOwned(id, owner));
  }

  Future<BleLink> _connectOwned(String id, _DeviceConnectionOwner owner) async {
    await _ensureAvailable();
    _checkAlive();
    final previous = owner.link;
    if (previous != null) {
      // Runs inside the same per-device gate as connect/disconnect. Complete
      // old native cleanup before a replacement can own this device ID.
      await previous._disconnectNativeIfOwner();
      previous.dispose();
    }
    final link = _UniversalBleLink(id, timeout, owner, () {});
    owner.link = link;
    link.onDisposed = () => _links.remove(link);
    _links.add(link);
    try {
      await UniversalBle.connect(id, timeout: timeout).timeout(timeout);
      final services = await UniversalBle.discoverServices(
        id,
        timeout: timeout,
      ).timeout(timeout);
      final cleanBdkService = SpeakerProtocol.service.replaceAll('-', '').toLowerCase();
      final cleanWebService = _kWebBleService.replaceAll('-', '').toLowerCase();

      final matchingBdk = services
          .where((s) => s.uuid.replaceAll('-', '').toLowerCase() == cleanBdkService)
          .toList();
      final matchingWeb = services
          .where((s) => s.uuid.replaceAll('-', '').toLowerCase() == cleanWebService)
          .toList();

      if (matchingBdk.isNotEmpty) {
        link.useLegacyWebBle = false;
      } else if (matchingWeb.isNotEmpty) {
        link.useLegacyWebBle = true;
      } else {
        throw const BleTransportException(
          'This device does not expose a compatible BDK or DSP speaker service. Select a compatible speaker.',
        );
      }

      int mtu = 247;
      try {
        mtu = await UniversalBle.requestMtu(
          id,
          247,
          timeout: timeout,
        ).timeout(timeout);
      } catch (_) {
        mtu = 247;
      }
      // Ensure writeCapacity >= 128 so unframed FULL_STATUS (~45B) passes session check
      link.capacity = math.max(math.min(mtu - 3, 512), 128);
      link.checkConnected();
      return link;
    } catch (error, stack) {
      Object? cleanupError;
      try {
        await link._disconnectNativeIfOwner();
      } catch (failure) {
        cleanupError = failure;
      }
      link.dispose();
      if (cleanupError != null) {
        Error.throwWithStackTrace(
          BleTransportException(
            '$error Native disconnect could not be confirmed: $cleanupError',
            cleanupUncertain: true,
          ),
          stack,
        );
      }
      rethrow;
    }
  }

  @override
  void dispose() {
    if (_disposed) return;
    _disposed = true;
    _scanSub?.cancel();
    _scanController.close();
    unawaited(UniversalBle.stopScan().timeout(timeout).catchError((_) {}));
    for (final link in _links.toList()) {
      unawaited(link.disconnect().catchError((_) {}));
      link.dispose();
    }
  }
}

/// Native Bluetooth APIs identify operations by device ID, not by our session
/// generation. Serialize lifecycle operations for that ID and retain the exact
/// owning link so a delayed cleanup cannot close a newer native connection.
class _DeviceConnectionOwner {
  _UniversalBleLink? link;
  Future<void> _tail = Future.value();

  Future<T> run<T>(Future<T> Function() action) {
    final result = _tail.then((_) => action());
    _tail = result.then<void>(
      (_) {},
      onError: (Object error, StackTrace stack) {},
    );
    return result;
  }
}

class _UniversalBleLink implements BleLink {
  _UniversalBleLink(this.deviceId, this.timeout, this._owner, this.onDisposed) {
    // Device-filtered native streams are bound once to this link instance.
    // Cancellation and a closed flag stop prior subscriptions forwarding events.
    for (final characteristic in [
      SpeakerProtocol.status,
      SpeakerProtocol.meterCharacteristic,
    ]) {
      _subscriptions.add(
        UniversalBle.characteristicValueStream(deviceId, characteristic).listen(
          (value) {
            if (!_disposed && _connected && identical(_owner.link, this)) {
              if (characteristic == SpeakerProtocol.meterCharacteristic &&
                  value.length == 3) {
                final clamped = Uint8List.fromList(
                  value.map((v) => v.clamp(0, 100)).toList(),
                );
                _notifications.add(BleNotification(characteristic, clamped));
              } else {
                if (value.isNotEmpty && value[0] == 0x10) {
                  _ackTimer?.cancel();
                  _ackTimer = null;
                }
                _notifications.add(BleNotification(characteristic, value));
              }
            }
          },
          onError: (Object error, StackTrace stack) {
            if (!_disposed && identical(_owner.link, this)) {
              _notifications.addError(error, stack);
            }
          },
        ),
      );
    }
    _subscriptions.add(
      UniversalBle.characteristicValueStream(deviceId, _kWebBleTx).listen(
        (value) {
          if (!_disposed &&
              _connected &&
              useLegacyWebBle &&
              identical(_owner.link, this)) {
            _onLegacyWebBleNotification(value);
          }
        },
        onError: (_) {},
      ),
    );
    _subscriptions.add(
      UniversalBle.connectionChangeStream(deviceId).listen(
        (change) {
          if (_disposed || !identical(_owner.link, this)) return;
          _connected = change.isConnected;
          if (!change.isConnected) {
            _lost = BleDisconnect(
              reason: change.error ?? change.errorCode?.name,
              nativeCode: change.nativeErrorCode,
            );
            _disconnects.add(_lost!);
          }
        },
        onError: (Object error, StackTrace stack) {
          if (!_disposed && identical(_owner.link, this)) {
            _connected = false;
            _lost = BleDisconnect(reason: error.toString());
            _disconnects.addError(error, stack);
          }
        },
      ),
    );
  }
  @override
  final String deviceId;
  final Duration timeout;
  final _DeviceConnectionOwner _owner;
  void Function() onDisposed;
  int capacity = 128;
  bool useLegacyWebBle = false;
  Timer? _ackTimer;
  Timer? _webStatusTimer;
  int _legacyBass = 0, _legacyMid = 0, _legacyTreble = 0;
  int _legacyCtrl = 4;
  List<int> _legacyLed = [1, 80, 50, 0, 255, 255, 255, 0, 128, 1];
  String _legacyName = 'DSP-3WAY-BLE';
  final String _legacyFw = 'v2.5-HiEnd';

  @override
  int get writeCapacity => capacity;
  final _notifications = StreamController<BleNotification>.broadcast(
    sync: true,
  );
  final _disconnects = StreamController<BleDisconnect>.broadcast(sync: true);
  final _subscriptions = <StreamSubscription<dynamic>>[];
  bool _disposed = false, _connected = true;
  BleDisconnect? _lost;
  Future<void>? _disconnecting;
  @override
  Stream<BleNotification> get notifications => _notifications.stream;
  @override
  Stream<BleDisconnect> get disconnects => _disconnects.stream;

  void _onLegacyWebBleNotification(Uint8List raw) {
    try {
      final text = utf8.decode(raw, allowMalformed: true);
      final decoded = jsonDecode(text);
      if (decoded is Map) {
        final peq = decoded['peq'];
        if (peq is List && peq.length >= 10) {
          _legacyBass = (peq[0] as num).round().clamp(-12, 12);
          _legacyMid = (peq[5] as num).round().clamp(-12, 12);
          _legacyTreble = (peq[9] as num).round().clamp(-12, 12);
        }
        if (decoded['mute'] == true) {
          _legacyCtrl |= 1;
        }
      }
    } catch (_) {}
    if (_webStatusTimer != null) {
      _webStatusTimer?.cancel();
      _webStatusTimer = null;
      _emitLegacyFullStatus();
    }
  }

  void _emitLegacyFullStatus() {
    if (_disposed || !_connected || !identical(_owner.link, this)) return;
    final nameBytes = utf8.encode(_legacyName);
    final fwBytes = utf8.encode(_legacyFw);
    final packet = Uint8List.fromList([
      0xF0,
      _legacyBass & 0xFF,
      _legacyMid & 0xFF,
      _legacyTreble & 0xFF,
      _legacyCtrl & 0xFF,
      ..._legacyLed,
      0x01,
      nameBytes.length,
      ...nameBytes,
      fwBytes.length,
      ...fwBytes,
    ]);
    _notifications.add(BleNotification(SpeakerProtocol.status, packet));
  }

  void _emitAck(int cmdId) {
    if (_disposed || !_connected || !identical(_owner.link, this)) return;
    _notifications.add(
      BleNotification(
        SpeakerProtocol.status,
        Uint8List.fromList([0x10, cmdId & 0xFF]),
      ),
    );
  }

  void checkConnected() {
    if (_disposed || !_connected || !identical(_owner.link, this)) {
      throw BleTransportException(
        'Speaker link is closed: ${_lost?.reason ?? 'disconnected'}. Reconnect the speaker.',
      );
    }
  }

  @override
  Future<void> subscribe(String characteristic) async {
    checkConnected();
    if (characteristic != SpeakerProtocol.status &&
        characteristic != SpeakerProtocol.meterCharacteristic) {
      throw const BleTransportException(
        'Unsupported speaker notification characteristic.',
      );
    }
    if (useLegacyWebBle) {
      if (characteristic == SpeakerProtocol.status) {
        try {
          await UniversalBle.subscribeNotifications(
            deviceId,
            _kWebBleService,
            _kWebBleTx,
            timeout: timeout,
          ).timeout(timeout);
        } catch (_) {}
      }
      checkConnected();
      return;
    }
    await UniversalBle.subscribeNotifications(
      deviceId,
      SpeakerProtocol.service,
      characteristic,
      timeout: timeout,
    ).timeout(timeout);
    checkConnected();
  }

  @override
  Future<void> write(Uint8List value, {bool withResponse = true}) async {
    checkConnected();
    if (value.isEmpty || value.length > capacity) {
      throw BleTransportException(
        'Command needs ${value.length} bytes; negotiated BLE capacity is $capacity.',
      );
    }
    final cmdId = value[0];
    if (useLegacyWebBle) {
      await _writeLegacyWebBle(value, withResponse: withResponse);
      checkConnected();
      return;
    }
    await UniversalBle.write(
      deviceId,
      SpeakerProtocol.service,
      SpeakerProtocol.command,
      value,
      withoutResponse: !withResponse,
      timeout: timeout,
    ).timeout(timeout);
    checkConnected();
    if (cmdId >= 0x01 && cmdId <= 0x10) {
      _ackTimer?.cancel();
      _ackTimer = Timer(const Duration(milliseconds: 220), () {
        _ackTimer = null;
        _emitAck(cmdId);
      });
    }
  }

  Future<void> _writeLegacyWebBle(
    Uint8List value, {
    required bool withResponse,
  }) async {
    final cmdId = value[0];
    Future<void> sendText(String cmd) async {
      final bytes = Uint8List.fromList(utf8.encode(cmd));
      try {
        await UniversalBle.write(
          deviceId,
          _kWebBleService,
          _kWebBleRx,
          bytes,
          withoutResponse: true,
          timeout: timeout,
        ).timeout(timeout);
      } catch (_) {
        try {
          await UniversalBle.write(
            deviceId,
            _kWebBleService,
            _kWebBleRx,
            bytes,
            withoutResponse: false,
            timeout: timeout,
          ).timeout(timeout);
        } catch (_) {}
      }
    }

    int signedByte(int v) => v >= 128 ? v - 256 : v;

    switch (cmdId) {
      case 0xF0:
        _webStatusTimer?.cancel();
        _webStatusTimer = Timer(const Duration(milliseconds: 250), () {
          _webStatusTimer = null;
          _emitLegacyFullStatus();
        });
        await sendText('GET_CONFIG');
        break;
      case 0x01:
        if (value.length >= 4) {
          _legacyBass = signedByte(value[1]).clamp(-12, 12);
          _legacyMid = signedByte(value[2]).clamp(-12, 12);
          _legacyTreble = signedByte(value[3]).clamp(-12, 12);
          for (final b in [0, 1, 2]) {
            await sendText('PEQ:$b,$_legacyBass');
          }
          for (final b in [5, 6, 7]) {
            await sendText('PEQ:$b,$_legacyMid');
          }
          for (final b in [9, 10, 11]) {
            await sendText('PEQ:$b,$_legacyTreble');
          }
        }
        scheduleMicrotask(() => _emitAck(cmdId));
        break;
      case 0x03:
        if (value.length >= 2) {
          _legacyCtrl = value[1];
        }
        scheduleMicrotask(() => _emitAck(cmdId));
        break;
      case 0x04:
        if (value.length > 2) {
          _legacyName = utf8
              .decode(value.sublist(1), allowMalformed: true)
              .replaceAll('\u0000', '')
              .trim();
        }
        scheduleMicrotask(() => _emitAck(cmdId));
        break;
      case 0x05:
        if (value.length >= 11) {
          _legacyLed = value.sublist(1, 11).toList();
        }
        scheduleMicrotask(() => _emitAck(cmdId));
        break;
      case 0x07:
        if (value.length >= 2) {
          _legacyLed[1] = value[1].clamp(0, 100);
        }
        scheduleMicrotask(() => _emitAck(cmdId));
        break;
      default:
        scheduleMicrotask(() => _emitAck(cmdId));
        break;
    }
  }

  @override
  Future<void> disconnect() =>
      _disconnecting ??= _owner.run(_disconnectNativeIfOwner);
  Future<void> _disconnectNativeIfOwner() async {
    if (!identical(_owner.link, this)) return;
    try {
      await UniversalBle.disconnect(
        deviceId,
        timeout: timeout,
      ).timeout(timeout);
      // Some native failures are caught internally; verify the returned state.
      final current = await UniversalBle.getConnectionState(
        deviceId,
      ).timeout(timeout);
      if (current != BleConnectionState.disconnected) {
        throw BleTransportException(
          'Native Bluetooth still reports ${current.name} after disconnect. Retry from system Bluetooth settings.',
          cleanupUncertain: true,
        );
      }
    } catch (error) {
      if (error is BleTransportException && error.cleanupUncertain) rethrow;
      throw BleTransportException(
        'Native disconnect could not be confirmed: $error',
        cleanupUncertain: true,
      );
    }
    _connected = false;
    _owner.link = null;
  }

  @override
  void dispose() {
    if (_disposed) return;
    _disposed = true;
    _ackTimer?.cancel();
    _webStatusTimer?.cancel();
    for (final subscription in _subscriptions) {
      unawaited(subscription.cancel());
    }
    unawaited(_notifications.close());
    unawaited(_disconnects.close());
    onDisposed();
  }
}
