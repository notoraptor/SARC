import logging
from dataclasses import dataclass
from pathlib import Path
from pprint import pprint

import gifnoc

from sarc.alerts.common import CheckStatus
from sarc.alerts.runner import CheckRunner
from sarc.config import config

logger = logging.getLogger(__name__)


@dataclass
class HealthCheckCommand:
    config: Path | None = None
    once: bool = False

    name: str | None = None

    def execute(self) -> int:
        hcfg = config().health_monitor
        assert hcfg is not None
        with gifnoc.use(self.config):
            if self.name is not None:
                # only run one check, once (no CheckRunner)
                check = hcfg.checks[self.name]
                results = check(write=False)
                pprint(results)
                for k, status in results.statuses.items():
                    assert isinstance(status, CheckStatus)
                    print(f"{status.name} -- {k}")
                print(f"{results.status.name}")
            elif self.once:
                # run all checks, once (no CheckRunner)
                for check in [c for c in hcfg.checks.values() if c.active]:
                    results = check(write=False)
                    if results.status == CheckStatus.OK:
                        print(f"Check '{check.name}' succeeded.")
                    else:
                        print(f"Check '{check.name}' failed.")
                        pprint(results)
                        for k, status in results.statuses.items():
                            assert isinstance(status, CheckStatus)
                            print(f"{status.name} -- {k}")
                        print(f"{results.status.name}")
            else:
                try:
                    runner = CheckRunner(directory=hcfg.directory, checks=hcfg.checks)
                    runner.start()
                except KeyboardInterrupt:
                    logger.info("Execution ended due to KeyboardInterrupt")
        return 0
