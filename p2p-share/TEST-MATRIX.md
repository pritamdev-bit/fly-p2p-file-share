# fly manual test matrix

Automated coverage (`cargo test`) owns the transfer core: roundtrips,
pause/cancel/resume, sender death, preview/receive overlap, duplicate
names, disk preflight math. This matrix is what only a human with two
machines (or VMs) can check. Run it before every tester handoff.

## Sleep (QUIC connections do not survive sleep)

| # | Scenario | Steps | Expected |
|---|----------|-------|----------|
| S1 | Receiver sleeps mid-transfer | Start a big download, sleep the receiver 1+ min, wake it | Transfer reports the sender went offline (or stalls, then reports it); **Resume continues from cache** — bytes already downloaded are not fetched again |
| S2 | Sender sleeps mid-transfer | Start a big download, sleep the sender 1+ min, wake it | Receiver reports the sender went offline; after wake, Resume with a **fresh ticket** (the sender's addresses may have changed) continues from cache |
| S3 | Sleep while idle | Sleep either side with no transfer running, wake it | App is responsive; a new share/receive works without restart |
| S4 | Lid-close during lookup | Paste a ticket, sleep before details appear, wake | Lookup errors or retries cleanly; editing/re-pasting the ticket recovers; Start is never stuck disabled |

## Core transfers

| # | Scenario | Steps | Expected |
|---|----------|-------|----------|
| T1 | Two-machine full ticket | Share on PC1, receive on PC2 (different networks ideally) | Bytes match; sender shows Delivered and auto-stops; both sides show the pipe (Direct/Relayed) next to the speed |
| T2 | Short (id) ticket | Same with `--ticket-type id` / short ticket | Works; if it fails where a full ticket works, suspect DNS filtering (the error says so) |
| T3 | Impatient start | Paste ticket, try Start before details load | Start stays disabled until the lookup finishes; no wedge, no restart needed |
| T4 | Double receive | Receive the same file twice into one folder | Second saves as `name (1).ext`; banner + Open use the real name |
| T5 | Full disk | Receive a file bigger than free space | Refused **before** downloading, with the needed/available amounts |
| T6 | Sender death | Kill the sender app mid-transfer | Receiver reports "sender went offline" promptly with progress saved; Resume works when the sender is back |

## Interruptions

| # | Scenario | Steps | Expected |
|---|----------|-------|----------|
| I1 | Pause → Resume | Pause mid-download, Resume | Continues from cache (fast-forward, no re-download) |
| I2 | Cancel → re-receive | Cancel mid-download, receive again | Starts from zero (cache was deleted) |
| I3 | App restart resume | Kill the receiver app mid-download, reopen, paste the same ticket | Continues from cache (cross-process resume) |
| I4 | Network switch | Switch the receiver's network (WiFi → hotspot) mid-transfer | Transfer retries on a fresh connection and completes |

## UX flows

| # | Scenario | Steps | Expected |
|---|----------|-------|----------|
| U1 | QR + deeplink | Scan the QR / open the share link / click `fly://` while running | Ticket lands in Receive (running app focuses, no second window) |
| U2 | Open / reveal | After a download, Open file + Show in folder | File opens; folder opens with the file selected (executables ask first) |
| U3 | Diagnostics | Open Diagnostics with relay blocked (VPN/proxy) and unblocked | Relay/NAT rows reflect reality; guidance notes appear for UDP-blocked and relay-unreachable |
| U4 | Quit guard | Try to quit mid-download | App asks for confirmation; quit anyway stops cleanly |
| U5 | Cache size + clear | Open Diagnostics after some transfers | Shows total size + folder count; Clear cache empties it and reports freed bytes; refused with guidance while a download runs |
