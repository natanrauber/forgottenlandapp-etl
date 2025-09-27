import 'dart:math';

import 'package:forgottenlandapp_adapters/adapters.dart';
import 'package:forgottenlandapp_models/models.dart';
import 'package:forgottenlandapp_utils/utils.dart';
import 'package:shelf/shelf.dart';
import 'package:shelf_router/shelf_router.dart';

abstract class IETL {
  Future<Response> expRecord(Request request);
  Future<Response> currentExp(Request request);
  Future<Response> expGainedToday(Request request);
  Future<Response> expGainedYesterday(Request request);
  Future<Response> expGainedLast7Days(Request request);
  Future<Response> expGainedLast30Days(Request request);
  Future<Response> expGainedLast365Days(Request request);
  Future<Response> registerOnlinePlayers(Request request);
  Future<Response> rookmaster(Request request);
  Future<Response> calcSkillPoints(Request request);
}

// Extract, Transform, Load.
class ETL implements IETL {
  ETL(this.env, this.databaseClient, this.httpClient);

  final Env env;
  final IDatabaseClient databaseClient;
  final IHttpClient httpClient;

  @override
  Future<Response> expRecord(Request request) async {
    databaseClient.setup(request.headers['supabaseUrl'], request.headers['supabaseKey']);
    if (await _exists('exp-record', <String, Object>{'date': DT.tibia.today()})) return ApiResponse.accepted();
    return _getCurrentExp('exp-record', 'insert');
  }

  @override
  Future<Response> currentExp(Request request) async {
    databaseClient.setup(request.headers['supabaseUrl'], request.headers['supabaseKey']);
    return _getCurrentExp('current-exp', 'update');
  }

  Future<Response> _getCurrentExp(String table, String operation) async {
    try {
      Record record = await _loadCurrentExp();
      if (record.list.isEmpty) return ApiResponse.noContent();
      await _saveCurrentExp(record, table, operation);
      return ApiResponse.success();
    } catch (e) {
      return ApiResponse.error(e);
    }
  }

  Future<Record> _loadCurrentExp() async {
    List<World> worlds = await _getWorlds();
    Record currentExp = Record(list: <HighscoresEntry>[]);

    for (World world in worlds) {
      Record? aux;
      int page = 1;
      int i = 0;
      bool retry = false;
      bool loadNext = false;

      do {
        await Future<void>.delayed(Duration(milliseconds: 500));

        if (retry) {
          i++;
        } else {
          i = 0;
        }

        aux = null;
        MyHttpResponse response = await httpClient.get(
          '${env['PATH_TIBIA_DATA_SELFHOSTED']}/highscores/$world/experience/none/$page',
        );

        if (response.success) {
          aux = Record.fromJson(response.dataAsMap['highscores'] as Map<String, dynamic>);
          aux.list.removeWhere((HighscoresEntry e) => (e.level ?? 0) < 30);
          currentExp.list.addAll(aux.list);
          page++;
        }

        retry = !response.success && i < 5;
        loadNext = aux?.list.isNotEmpty == true && (aux?.list.last.level ?? 0) > 30;
        // loadNext = false;
      } while (retry || loadNext);
    }

    currentExp.list.sort((HighscoresEntry a, HighscoresEntry b) => (b.value ?? 0).compareTo(a.value ?? 0));
    return currentExp;
  }

  Future<List<World>> _getWorlds() async {
    final MyHttpResponse response = await httpClient.get('${env['PATH_TIBIA_DATA']}/worlds');

    if (response.dataAsMap['worlds'] is! Map) return <World>[];
    if (response.dataAsMap['worlds']['regular_worlds'] is! List) return <World>[];

    List<dynamic> data = response.dataAsMap['worlds']['regular_worlds'];
    List<World> worlds = <World>[];

    for (dynamic e in data) {
      if (e is Map<String, dynamic>) worlds.add(World.fromJson(e));
    }
    return worlds;
  }

  Future<dynamic> _saveCurrentExp(Record record, String table, String operation) async {
    if (operation == 'update') {
      Map<String, dynamic> values = <String, dynamic>{
        'data': record.toJson(),
        'timestamp': DT.germany.timeStamp(),
      };
      return databaseClient.from(table).update(values).match(<String, Object>{'world': 'All'});
    }
    Map<String, dynamic> values = <String, dynamic>{
      'date': DT.tibia.today(),
      'world': 'All',
      'data': record.toJson(),
      'timestamp': DT.germany.timeStamp(),
    };
    return databaseClient.from(table).insert(values);
  }

  @override
  Future<Response> expGainedToday(Request request) async {
    databaseClient.setup(request.headers['supabaseUrl'], request.headers['supabaseKey']);

    try {
      Record result = await _calcExpGainToday();
      _recordAddMissingRank(result);
      if (result.list.isEmpty) return ApiResponse.noContent();
      await _saveExpGain('exp-gain-today', DT.tibia.today(), result, deleteOlder: true, canUpdate: true);
      return ApiResponse.success();
    } catch (e) {
      return ApiResponse.error(e);
    }
  }

  Future<Record> _calcExpGainToday() async {
    Record start = await _getWhere('exp-record', DT.tibia.today());
    dynamic response = await databaseClient.from('current-exp').select().single();
    Record end = Record.fromJson(response['data']);
    Record result = Record(list: <HighscoresEntry>[]);
    result.list.addAll(_getExpDiff(start, end));
    result.list.sort((HighscoresEntry a, HighscoresEntry b) => (b.value ?? 0).compareTo(a.value ?? 0));
    return result;
  }

  @override
  Future<Response> expGainedYesterday(Request request) async {
    databaseClient.setup(request.headers['supabaseUrl'], request.headers['supabaseKey']);

    if (await _exists('exp-gain-last-day', <String, Object>{'date': DT.tibia.yesterday()})) {
      return ApiResponse.accepted();
    }

    try {
      Record result = await _getExpGainRange(DT.tibia.yesterday(), DT.tibia.today());
      _recordAddMissingRank(result);
      if (result.list.isEmpty) return ApiResponse.noContent();
      await _saveExpGain('exp-gain-last-day', DT.tibia.yesterday(), result, deleteOlder: false);
      return ApiResponse.success();
    } catch (e) {
      return ApiResponse.error(e);
    }
  }

  @override
  Future<Response> expGainedLast7Days(Request request) async {
    databaseClient.setup(request.headers['supabaseUrl'], request.headers['supabaseKey']);

    if (await _exists('exp-gain-period', <String, Object>{'period': '7days', 'date': DT.tibia.yesterday()})) {
      return ApiResponse.accepted();
    }

    try {
      Record result = await _getExpGainRange(DT.tibia.aWeekAgo(), DT.tibia.today());
      _recordAddMissingRank(result);
      if (result.list.isEmpty) return ApiResponse.noContent();
      await _saveExpGainPeriod('7days', DT.tibia.yesterday(), result);
      return ApiResponse.success();
    } catch (e) {
      return ApiResponse.error(e);
    }
  }

  @override
  Future<Response> expGainedLast30Days(Request request) async {
    databaseClient.setup(request.headers['supabaseUrl'], request.headers['supabaseKey']);

    if (await _exists('exp-gain-period', <String, Object>{'period': '30days', 'date': DT.tibia.yesterday()})) {
      return ApiResponse.accepted();
    }

    try {
      Record result = await _getExpGainRange(DT.tibia.aMonthAgo(), DT.tibia.today());
      _recordAddMissingRank(result);
      if (result.list.isEmpty) return ApiResponse.noContent();
      await _saveExpGainPeriod('30days', DT.tibia.yesterday(), result);
      return ApiResponse.success();
    } catch (e) {
      return ApiResponse.error(e);
    }
  }

  @override
  Future<Response> expGainedLast365Days(Request request) async {
    databaseClient.setup(request.headers['supabaseUrl'], request.headers['supabaseKey']);

    if (await _exists('exp-gain-period', <String, Object>{'period': '365days', 'date': DT.tibia.yesterday()})) {
      return ApiResponse.accepted();
    }

    try {
      Record result = await _getExpGainRange(DT.tibia.aYearAgo(), DT.tibia.today());
      _recordAddMissingRank(result);
      if (result.list.isEmpty) return ApiResponse.noContent();
      await _saveExpGainPeriod('365days', DT.tibia.yesterday(), result);
      return ApiResponse.success();
    } catch (e) {
      return ApiResponse.error(e);
    }
  }

  Future<Record> _getExpGainRange(String startDate, String endDate) async {
    Record start = await _getWhere('exp-record', startDate);
    Record end = await _getWhere('exp-record', endDate);
    Record result = Record(list: <HighscoresEntry>[]);
    result.list.addAll(_getExpDiff(start, end));
    result.list.sort((HighscoresEntry a, HighscoresEntry b) => (b.value ?? 0).compareTo(a.value ?? 0));
    return result;
  }

  Future<Record> _getWhere(String table, String date) async {
    dynamic response = await databaseClient.from(table).select().eq('date', date).single();
    return Record.fromJson(response['data']);
  }

  List<HighscoresEntry> _getExpDiff(Record yesterday, Record today) {
    List<HighscoresEntry> newList = <HighscoresEntry>[];

    for (final HighscoresEntry t in today.list) {
      if (_isValidEntry(t, yesterday)) {
        HighscoresEntry y = yesterday.list.firstWhere((HighscoresEntry v) => v.name == t.name);
        t.value = t.value! - y.value!;
        if (t.value! > 0) newList.add(t);
      }
    }

    return newList;
  }

  bool _isValidEntry(HighscoresEntry t, Record record) {
    if (t.value is! int) return false;
    if (!record.list.any((HighscoresEntry y) => y.name == t.name)) return false;
    return record.list.firstWhere((HighscoresEntry y) => y.name == t.name).value is int;
  }

  void _recordAddMissingRank(Record record) {
    if (record.list.isEmpty) return;
    if (record.list.first.rank != null) return;
    for (HighscoresEntry e in record.list) {
      e.rank = record.list.indexOf(e) + 1;
    }
  }

  Future<void> _saveExpGain(
    String table,
    String date,
    Record data, {
    required bool deleteOlder,
    bool canUpdate = false,
  }) async {
    Map<String, dynamic> values = <String, dynamic>{
      'date': date,
      'world': 'All',
      'data': data.toJson(),
      'timestamp': DT.germany.timeStamp(),
    };
    if (deleteOlder) await databaseClient.from(table).delete().neq('date', date);
    if (canUpdate) return databaseClient.from(table).upsert(values);
    return databaseClient.from(table).insert(values);
  }

  Future<void> _saveExpGainPeriod(String period, String date, Record data) async {
    Map<String, dynamic> values = <String, dynamic>{
      'period': period,
      'date': date,
      'data': data.toJson(),
      'timestamp': DT.germany.timeStamp(),
    };
    await databaseClient.from('exp-gain-period').delete().eq('period', period).neq('date', date);
    return databaseClient.from('exp-gain-period').insert(values);
  }

  Future<bool> _exists(String table, Map<String, Object> query) async {
    List<dynamic> response = await databaseClient.from(table).select().match(query);
    return response.isNotEmpty;
  }

  @override
  Future<Response> registerOnlinePlayers(Request request) async {
    try {
      databaseClient.setup(request.headers['supabaseUrl'], request.headers['supabaseKey']);

      Online onlineNow = await _getOnlineNow();
      if (onlineNow.list.isEmpty) return ApiResponse.noContent();

      await _saveOnlineNow(onlineNow);
      await _saveOnlineTimeToday(await _getOnlineTimeToday(onlineNow));
      await _saveOnlineTimePeriod('onlinetime-last7days', Duration(days: 7));
      await _saveOnlineTimePeriod('onlinetime-last30days', Duration(days: 30));
      await _saveOnlineTimePeriod('onlinetime-last365days', Duration(days: 365));

      return ApiResponse.success();
    } catch (e) {
      return ApiResponse.error(e);
    }
  }

  Future<Online> _getOnlineNow() async {
    List<World> worlds = await _getWorlds();
    Online online = Online(list: <OnlineEntry>[]);

    for (World world in worlds) {
      MyHttpResponse response;
      int i = 1;

      do {
        response = await httpClient.get('${env['PATH_TIBIA_DATA']}/world/$world');
        if (response.success) {
          Online aux = Online.fromJsonTibiaDataAPI(response.dataAsMap);
          aux.list.removeWhere((OnlineEntry e) => e.vocation != 'None' || (e.level ?? 0) < 10);
          for (OnlineEntry e in aux.list) {
            e.world = world.name;
          }
          online.list.addAll(aux.list);
        }
      } while (!response.success && i != 5);
    }

    online.list.sort((OnlineEntry a, OnlineEntry b) => (b.level ?? 0).compareTo(a.level ?? 0));
    return online;
  }

  Future<dynamic> _saveOnlineNow(Online online) async {
    Map<String, dynamic> values = <String, dynamic>{'data': online.toJson(), 'timestamp': DT.germany.timeStamp()};
    return databaseClient.from('online').update(values).match(<String, Object>{'world': 'All'});
  }

  int _compareTo(OnlineEntry a, OnlineEntry b) {
    if (a.time != b.time) return b.time.compareTo(a.time);
    return (b.level ?? 0).compareTo((a.level ?? 0));
  }

  void _onlineAddMissingRank(Online record) {
    if (record.list.first.rank != null) return;
    for (OnlineEntry e in record.list) {
      e.rank = record.list.indexOf(e) + 1;
    }
  }

  Future<dynamic> _saveOnlineTimeToday(Online online) async {
    Map<String, dynamic> values = <String, dynamic>{
      'date': DT.tibia.today(),
      'data': online.toJson(),
      'timestamp': DT.germany.timeStamp(),
    };
    return databaseClient.from('onlinetime').upsert(values).match(<String, Object>{'date': DT.tibia.now()});
  }

  Future<Online> _getOnlineTimeToday(Online onlineNow) async {
    onlineNow.list.removeWhere((OnlineEntry e) => (e.level ?? 0) < 10);
    List<dynamic> response = await databaseClient.from('onlinetime').select().eq('date', DT.tibia.today());
    Online result;

    if (response.isEmpty) {
      result = Online(list: onlineNow.list);
    } else {
      result = Online.fromJson(response.first['data']);
      for (OnlineEntry now in onlineNow.list) {
        if (result.list.any((OnlineEntry e) => e.name == now.name)) {
          result.list.firstWhere((OnlineEntry e) => e.name == now.name).time += 5;
          result.list.firstWhere((OnlineEntry e) => e.name == now.name).level = now.level;
        } else {
          result.list.add(now);
        }
      }
    }

    result.list.sort(_compareTo);
    _onlineAddMissingRank(result);
    return result;
  }

  Future<void> _saveOnlineTimePeriod(String table, Duration period) async {
    Online? onlineTime = await _getOnlineTimePeriod(table, period);
    if (onlineTime == null) return;

    Map<String, dynamic> values = <String, dynamic>{
      'date': DT.tibia.yesterday(),
      'data': onlineTime.toJson(),
      'timestamp': DT.germany.timeStamp(),
    };
    await databaseClient.from(table).insert(values);
    await databaseClient.from(table).delete().neq('date', DT.tibia.yesterday());
  }

  Future<Online?> _getOnlineTimePeriod(String table, Duration period) async {
    if (await _exists(table, <String, Object>{'date': DT.tibia.yesterday()})) return null;

    DateTime start = DT.tibia.now().subtract(period);
    DateTime end = DT.tibia.now().subtract(Duration(days: 1));
    Online result = Online(list: <OnlineEntry>[]);

    for (String date in DT.tibia.range(start, end)) {
      List<dynamic> response = await databaseClient.from('onlinetime').select().eq('date', date);

      if (response.isNotEmpty) {
        Online onlineTimeOnDate = Online.fromJson(response.first['data']);
        for (OnlineEntry dayE in onlineTimeOnDate.list) {
          if (result.list.any((OnlineEntry resultE) => resultE.name == dayE.name)) {
            result.list.firstWhere((OnlineEntry resultE) => resultE.name == dayE.name).time += dayE.time;
            result.list.firstWhere((OnlineEntry resultE) => resultE.name == dayE.name).level = dayE.level;
            result.list.firstWhere((OnlineEntry resultE) => resultE.name == dayE.name).world = dayE.world;
          } else {
            result.list.add(dayE);
          }
        }
      }
    }

    result.list.sort(_compareTo);
    _onlineAddMissingRank(result);
    return result;
  }

  @override
  Future<Response> rookmaster(Request request) async {
    databaseClient.setup(request.headers['supabaseUrl'], request.headers['supabaseKey']);
    if (await _exists('rook-master', <String, Object>{'date': DT.tibia.today()})) return ApiResponse.accepted();
    return _getRookMaster('rook-master', 'insert');
  }

  Future<Response> _getRookMaster(String table, String operation) async {
    try {
      Record record = await _calcRookMaster();
      _recordAddMissingRank(record);
      if (record.list.isEmpty) return ApiResponse.noContent();
      await _saveCurrentExp(record, table, operation);
      return ApiResponse.success();
    } catch (e) {
      return ApiResponse.error(e);
    }
  }

  Future<Record> _calcRookMaster() async {
    Record record = await _getLevel();

    Record fistRecord = await _getSkillRecord('fist');
    _addSkill('fist', record, fistRecord);

    Record axeRecord = await _getSkillRecord('axe');
    _addSkill('axe', record, axeRecord);

    Record clubRecord = await _getSkillRecord('club');
    _addSkill('club', record, clubRecord);

    Record swordRecord = await _getSkillRecord('sword');
    _addSkill('sword', record, swordRecord);

    Record distanceRecord = await _getSkillRecord('distance');
    _addSkill('distance', record, distanceRecord);

    Record shieldingRecord = await _getSkillRecord('shielding');
    _addSkill('shielding', record, shieldingRecord);

    Record fishingRecord = await _getSkillRecord('fishing');
    _addSkill('fishing', record, fishingRecord);

    record.list.sort((HighscoresEntry a, HighscoresEntry b) => (b.value ?? 0).compareTo(a.value ?? 0));
    _rookmasterAddMissingRank(record);
    return record;
  }

  Future<Record> _getLevel() async {
    Record record = Record(list: <HighscoresEntry>[]);

    Record? aux;
    int page = 1;
    int i = 0;
    bool retry = false;
    bool loadNext = false;

    do {
      await Future<void>.delayed(Duration(seconds: 1));
      if (retry) {
        i++;
      } else {
        i = 0;
      }

      aux = null;
      MyHttpResponse response =
          await httpClient.get('${env['PATH_TIBIA_DATA_SELFHOSTED']}/highscores/all/experience/none/$page');

      if (response.success) {
        aux = Record.fromJsonExpanded(response.dataAsMap['highscores'] as Map<String, dynamic>);
        record.list.addAll(aux.list);
        page++;
      }

      retry = !response.success && i < 5;
      loadNext = aux?.list.isNotEmpty == true && page <= 20;
    } while (retry || loadNext);

    for (HighscoresEntry e in record.list) {
      int position = record.list.indexOf(e) + 1;
      int points = 1000 - (position - 1);
      e.expanded?.experience.position = position;
      e.expanded?.experience.points = points;
      e.value = points;
    }

    return record;
  }

  Future<Record> _getSkillRecord(String skill) async {
    Record record = Record(list: <HighscoresEntry>[]);

    Record? aux;
    int page = 1;
    int i = 0;
    bool retry = false;
    bool loadNext = false;

    do {
      await Future<void>.delayed(Duration(milliseconds: 500));
      if (retry) {
        i++;
      } else {
        i = 0;
      }

      aux = null;
      MyHttpResponse response =
          await httpClient.get('${env['PATH_TIBIA_DATA_SELFHOSTED']}/highscores/all/$skill/none/$page');

      if (response.success) {
        aux = Record.fromJson(response.dataAsMap['highscores'] as Map<String, dynamic>);
        record.list.addAll(aux.list);
        page++;
      }

      retry = !response.success && i < 5;
      loadNext = aux?.list.isNotEmpty == true && page <= 20;
    } while (retry || loadNext);

    return record;
  }

  void _addSkill(String name, Record record, Record skillRecord) {
    for (HighscoresEntry e in record.list) {
      if (skillRecord.list.any((HighscoresEntry se) => se.name == e.name)) {
        int? value = skillRecord.list.firstWhere((HighscoresEntry se) => se.name == e.name).value;
        int? position = skillRecord.list.firstWhere((HighscoresEntry se) => se.name == e.name).rank;
        if (value != null && position != null) {
          int points = 1000 - (position - 1);
          e.expanded?.updateFromJson(
            <String, Map<String, int>>{
              name: <String, int>{
                'value': value,
                'position': position,
                'points': points,
              },
            },
          );
          e.value = (e.value ?? 0) + points;
        }
      }
    }
  }

  int _calcSkillPoints(String name, int? value) {
    Map<String, int> a = <String, int>{
      'fist': 50,
      'axe': 50,
      'club': 50,
      'sword': 50,
      'distance': 25,
      'shielding': 100,
      'fishing': 20,
    };

    Map<String, double> b = <String, double>{
      'fist': 1.5,
      'axe': 2.0,
      'club': 2.0,
      'sword': 2.0,
      'distance': 2.0,
      'shielding': 1.5,
      'fishing': 1.1,
    };

    int c = 10;

    Map<String, int> d = <String, int>{
      'fist': 1800,
      'axe': 1800,
      'club': 1800,
      'sword': 1800,
      'distance': 1000,
      'shielding': 3600,
      'fishing': 1200,
    };

    return ((((pow(b[name]!, (value ?? c) - c) - 1) / (b[name]! - 1)) * a[name]!) / d[name]!).floor();
  }

  @override
  Future<Response> calcSkillPoints(Request request) async {
    databaseClient.setup(request.headers['supabaseUrl'], request.headers['supabaseKey']);

    try {
      String? name = request.params['name'] ?? '';
      int value = int.tryParse(request.params['value'] ?? '') ?? 0;
      int points = _calcSkillPoints(name, value);
      return ApiResponse.success(data: points);
    } catch (e) {
      return ApiResponse.error(e);
    }
  }

  void _rookmasterAddMissingRank(Record record) {
    for (HighscoresEntry e in record.list) {
      e.rank = record.list.indexOf(e) + 1;
    }
  }
}
