"""Read-only live check of the SAP InfoSet adapter.

Run from the project root with ``python -m tests.sap_live_check``.
"""

from __future__ import annotations

import argparse

import httpx
from pydantic import ValidationError

from thirdparty.sap.utils import SAPClient


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--infoset", default="TESTJOIN")
    parser.add_argument("--ca-bundle", help="Trusted SAP certificate or CA bundle in PEM format")
    args = parser.parse_args()

    try:
        with SAPClient(timeout=15, ca_bundle=args.ca_bundle) as sap:
            result = sap.get_infoset_query_details(args.infoset)
    except httpx.ConnectError as exc:
        if "CERTIFICATE_VERIFY_FAILED" in str(exc):
            print("TLS verification failed. Set SAP_CA_BUNDLE in .env or pass --ca-bundle PATH.")
        else:
            print(f"Connection failed: {type(exc).__name__}: {exc}")
        return 2
    except httpx.HTTPStatusError as exc:
        print(f"SAP returned HTTP {exc.response.status_code}.")
        return 2
    except ValidationError as exc:
        print("SAP responded, but its InfoSet payload did not match the adapter model:")
        for error in exc.errors():
            print(f"  {'.'.join(map(str, error['loc']))}: {error['type']}")
        return 2
    except (ValueError, OSError) as exc:
        print(f"Configuration error: {exc}")
        return 2

    print(f"InfoSet: {result.infoset}")
    print(f"SQL template complete: {result.sqlTemplateComplete}")
    print(f"Tables ({len(result.tables)}):")
    for table in result.tables:
        print(f"  {table.tableName}: {table.usage}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
