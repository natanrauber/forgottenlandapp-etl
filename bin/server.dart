import 'dart:io';

import 'package:dotenv/dotenv.dart';
import 'package:forgottenlandapp_adapters/adapters.dart';
import 'package:forgottenlandapp_utils/utils.dart';
import 'package:shelf/shelf.dart';
import 'package:shelf/shelf_io.dart';
import 'package:shelf_cors_headers/shelf_cors_headers.dart';
import 'package:shelf_router/shelf_router.dart';

import '../lib/src/etl.dart';

final List<EnvVar> _required = <EnvVar>[
  EnvVar.databaseKey,
  EnvVar.databaseUrl,
  EnvVar.pathTibiaDataApi,
  EnvVar.pathTibiaDataApiSelfHosted,
];
late final Env _env;
late final IDatabaseClient _databaseClient;
final IHttpClient _httpClient = MyDioClient();

Future<void> _loadEnv() async {
  Map<String, String> localMap = <String, String>{}..addAll(Platform.environment);
  final DotEnv dotEnv = DotEnv();
  dotEnv.load();
  // ignore: invalid_use_of_visible_for_testing_member
  localMap.addAll(dotEnv.map);
  _env = Env(env: localMap, required: _required);
}

// Configure routes.
final Router _router = Router()
  ..get('/exprecord', ETL(_env, _databaseClient, _httpClient).expRecord)
  ..get('/currentexp', ETL(_env, _databaseClient, _httpClient).currentExp)
  ..get('/expgain+today', ETL(_env, _databaseClient, _httpClient).expGainedToday)
  ..get('/expgain+yesterday', ETL(_env, _databaseClient, _httpClient).expGainedYesterday)
  ..get('/expgain+last7days', ETL(_env, _databaseClient, _httpClient).expGainedLast7Days)
  ..get('/expgain+last30days', ETL(_env, _databaseClient, _httpClient).expGainedLast30Days)
  ..get('/expgain+last365days', ETL(_env, _databaseClient, _httpClient).expGainedLast365Days)
  ..get('/online', ETL(_env, _databaseClient, _httpClient).registerOnlinePlayers)
  ..get('/rookmaster', ETL(_env, _databaseClient, _httpClient).rookmaster)
  ..get('/skill/<name>/<value>', ETL(_env, _databaseClient, _httpClient).calcSkillPoints);

void main(List<String> args) async {
  await _loadEnv();
  _databaseClient = MySupabaseClient(
    databaseUrl: _env[EnvVar.databaseUrl]!,
    databaseKey: _env[EnvVar.databaseKey]!,
  );

  // Use any available host or container IP (usually `0.0.0.0`).
  final InternetAddress ip = InternetAddress.anyIPv4;

  // Configure a pipeline that logs requests.
  final Handler handler = Pipeline().addMiddleware(corsHeaders()).addMiddleware(logRequests()).addHandler(_router.call);

  // For running in containers, we respect the PORT environment variable.
  final int port = int.parse(Platform.environment['PORT'] ?? '8080');
  final HttpServer server = await serve(handler, ip, port);
  print('Server listening on port ${server.port}');
}
