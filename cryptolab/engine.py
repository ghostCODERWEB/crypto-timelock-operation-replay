"""Replay scheduling, cancellation and execution with predecessor dependencies, grace windows and permanent operation IDs."""

from .common import (
    number,
    integer,
    identity,
    records,
    chronology,
    allocate,
    result,
    validate_document,
)
from decimal import Decimal


def _analyze(d):
    delay = integer(d["minimum_delay"])
    grace = integer(d["grace_period"], 1)
    operations = {}
    trace = []
    for e in chronology(d["events"]):
        oid = identity(e["id"])
        kind = e["kind"]
        t = e["timestamp"]
        if kind == "schedule":
            if oid in operations:
                raise ValueError("operation id already used")
            eta = integer(e["eta"])
            pred = e.get("predecessor")
            if eta < t + delay or pred == oid:
                raise ValueError("invalid schedule")
            if pred is not None:
                identity(pred)
            operations[oid] = {"eta": eta, "predecessor": pred, "state": "scheduled"}
        else:
            if oid not in operations or operations[oid]["state"] != "scheduled":
                raise ValueError("operation not scheduled")
            op = operations[oid]
            if kind == "cancel":
                op["state"] = "cancelled"
            elif kind == "execute":
                if t < op["eta"] or t > op["eta"] + grace:
                    raise ValueError("outside execution window")
                if (
                    op["predecessor"] is not None
                    and operations.get(op["predecessor"], {}).get("state") != "executed"
                ):
                    raise ValueError("predecessor incomplete")
                op["state"] = "executed"
            else:
                raise ValueError("unknown lifecycle event")
        trace.append({"id": oid, "timestamp": t, "state": operations[oid]["state"]})
    return {
        "operations": operations,
        "trace": trace,
        "executed_count": sum(o["state"] == "executed" for o in operations.values()),
    }


def analyze(document):
    validate_document(document)
    try:
        return result(_analyze(document))
    except (KeyError, TypeError, ArithmeticError, RecursionError) as error:
        raise ValueError("invalid domain input: " + str(error)) from error
