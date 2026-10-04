"""Проверить окружение Viking Tools без остановки бота и изменения профиля."""

import json
import logging
import sys

from viking_tools import Maintenance, TARIFFS, ToolsConfig, ToolsRecoveryEngine


def main():
    if hasattr(sys.stdout, 'reconfigure'):
        sys.stdout.reconfigure(encoding='utf-8', errors='replace')
    config = ToolsConfig()
    engine = ToolsRecoveryEngine(logging.getLogger('viking_tools_check'))
    engine.validate()
    info = config.dash.call('')
    maintenance = Maintenance(engine, config.state_dir / 'maintenance.json').call('Inspect')
    result = {
        'server': info['server'], 'profile': engine.profile_path.name,
        'telegram_ready': info['telegram_ready'],
        'templates': {TARIFFS[price][0]: len(config.template(price)) for price in TARIFFS},
        'restart_tasks': maintenance['tasks'],
    }
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == '__main__':
    main()
