#!/usr/bin/env bash
# StreamLake demo — the demo queries via the `pulsar-admin streamlake query` CLI.
#   default : print the ready-to-run commands (copy/paste to play with them)
#   --run   : execute all four against the running server
# Env: PULSAR_HOME (required), SL_NAMESPACE (default public/default).
set -euo pipefail
PULSAR_HOME="${PULSAR_HOME:?set PULSAR_HOME to the unpacked Pulsar dir}"
NS="${SL_NAMESPACE:-public/default}"
ADMIN="$PULSAR_HOME/bin/pulsar-admin"
MODE="print"
[ "${1:-}" = "--run" ] && MODE="run"

# All four are bounded (selective predicates / LIMIT) so they return quickly and print nicely even
# over a multi-TB table. Widen or drop the personId ranges to scan more; add --json for raw output.
Q_JOIN="SELECT * FROM Person p JOIN Employee e ON p.personId = e.personId WHERE p.personId BETWEEN 0 AND 100000 AND p.age BETWEEN 30 AND 40 AND e.salary >= 40000"
Q_GROUP="SELECT age, COUNT(*), MIN(personId), MAX(personId) FROM Person WHERE personId BETWEEN 0 AND 1000000 GROUP BY age"
Q_ORDER="SELECT personId, salary FROM Employee WHERE personId BETWEEN 0 AND 1000000 ORDER BY salary DESC LIMIT 20"
Q_SCAN="SELECT personId, name, age FROM Person WHERE personId BETWEEN 0 AND 20"

emit() {  # emit <title> <sql>
  local title="$1" sql="$2"
  echo "# --- $title ---"
  if [ "$MODE" = "run" ]; then
    "$ADMIN" streamlake query "$NS" "$sql"
  else
    printf '%s streamlake query %s "%s"\n' "$ADMIN" "$NS" "$sql"
  fi
  echo
}

emit "Inner join  (Person JOIN Employee ON personId; a pruned personId slice)" "$Q_JOIN"
emit "Group by    (age -> COUNT/MIN/MAX; scan a slice, tiny result)" "$Q_GROUP"
emit "Order by    (top-20 salaries; bounded top-K)" "$Q_ORDER"
emit "Scan        (a key-range slice of Person to eyeball the data)" "$Q_SCAN"

if [ "$MODE" = "print" ]; then
  echo "Run all four:  $(basename "$0") --run     (append --json to any query for raw JSON)"
  echo "Full-table group-by (heavy scan, 50-row result):"
  echo "  $ADMIN streamlake query $NS \"SELECT age, COUNT(*) FROM Person GROUP BY age\""
fi
