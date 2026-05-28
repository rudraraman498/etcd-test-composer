#!/usr/bin/env -S python3 -u


#anytime driver: Watch event mod_revision monotonicity check.

#Property asserted:
# Watch events received for a key prefix must arrive in monotonically non-decreasing mod_revision order.
# A decrease means events arrived out of order - a watch correctness violation.


import sys, time, threading
sys.path.append("/opt/antithesis/resources")
import helper

from antithesis.assertions import always, reachable, sometimes

WATCH_PREFIX = "anytime-watch/"
COLLECT_SECONDS = 3


def generate_traffic(prefix):
    """Write a few keys under the watch prefix to produce watch events."""
    client = helper.connect_to_host()
    for _ in range(5):
        key = prefix + helper.generate_random_string()
        value = helper.generate_random_string()
        helper.put_request(client, key, value)


def collect_events(prefix, duration):
    """
    Watch the prefix for "duration" seconds and return all received events.
    Each entry records mod_revision for ordering checks.
    """
    collected = []
    done = threading.Event()

    def _watch():
        try:
            client = helper.connect_to_host()
            events_iterator, cancel = client.watch_prefix(prefix)
            for event in events_iterator:
                collected.append({
                    "key":           event.key.decode("utf-8"),
                    "mod_revision":  event.mod_revision,
                })
                if done.is_set():
                    cancel()
                    break
        except Exception as e:
            print(f"anytime: watch error: {e}")

    #watcher runs in background thread
    t = threading.Thread(target=_watch, daemon=True)
    t.start()

    #generate traffic while watching so events actually arrive
    generate_traffic(prefix)

    time.sleep(duration)
    done.set()
    t.join(timeout=2)
    return collected


def check_revision_ordering(events):
    """
    Core assertion: mod_revision must be monotonically non-decreasing.
    A watch that delivers events out of revision order violates etcd's documented guarantee that watch events are ordered by revision.
    """
    for i in range(1, len(events)):
        prev = events[i - 1]
        curr = events[i]

        always(
            curr["mod_revision"] >= prev["mod_revision"],
            "Watch events are delivered in monotonically non-decreasing revision order",
            {
                "event_index":       i,
                "prev_key":          prev["key"],
                "prev_mod_revision": prev["mod_revision"],
                "curr_key":          curr["key"],
                "curr_mod_revision": curr["mod_revision"],
            }
        )


if __name__ == "__main__":
    print("anytime: starting watch revision check")

    #Runs the full collect cycle: opens watch, generates traffic, waits, returns events
    events = collect_events(WATCH_PREFIX, COLLECT_SECONDS)
    print(f"anytime: collected {len(events)} watch events")

    #this condition should be true at least once across all test runs in Antithesis
    sometimes(
        len(events) >= 2,
        "Anytime watch check collected enough events to verify ordering",
        {"event_count": len(events)}
    )

    if len(events) >= 2:
        #reachable assertion confirms to Antithesis that this code path was reached at least once
        reachable(
            "Watch event ordering check is being performed",
            {"event_count": len(events)}
        )
        check_revision_ordering(events)

    #final confirmation in container logs that the driver ran to completion without crashing
    print("anytime: watch revision check complete")