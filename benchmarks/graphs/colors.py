autoquorum_color = "tab:orange"
wread_color = "tab:blue"
bread_color = "tab:green"
qread_color = "tab:purple"
mixed_color = "tab:cyan"
epaxos_color = "tab:gray"
epaxos_slow_color = "black"
# epaxos_slow_color = plt.colormaps.get_cmap("tab20")(7)
strat_colors = {
    "AutoQuorum": autoquorum_color,
    "Baseline": wread_color,
    "Static LA Leader": wread_color,
    "ReadAsWrite": wread_color,
    "RAW": wread_color,
    "DQR": bread_color,
    "BallotRead": bread_color,
    "QuorumRead": qread_color,
    "Mixed": mixed_color,
    "EPaxos": epaxos_color,
    "EPaxos fast path": epaxos_color,
    "EPaxos slow path": epaxos_slow_color,
    "Etcd": "tab:red",
}

autoquorum_marker = "s"
wread_marker = "D"
bread_marker = "o"
qread_marker = "h"
mixed_marker = "X"
epaxos_marker = "^"
epaxos_slow_marker = "v"
strat_markers = {
    "AutoQuorum": autoquorum_marker,
    "Baseline": wread_marker,
    "ReadAsWrite": wread_marker,
    "RAW": wread_marker,
    "DQR": bread_marker,
    "BallotRead": bread_marker,
    "QuorumRead": qread_marker,
    "Mixed": mixed_marker,
    "EPaxos": epaxos_marker,
    "EPaxos fast path": epaxos_marker,
    "EPaxos slow path": epaxos_slow_marker,
    "Etcd": ">",
}

autoquorum_hatch = "x"
wread_hatch = "-"
bread_hatch = "/"
qread_hatch = "\\"
mixed_hatch = "+"
epaxos_hatch = "."
epaxos_slow_hatch = "o"
strat_hatches = {
    "AutoQuorum": autoquorum_hatch,
    "Baseline": wread_hatch,
    "ReadAsWrite": wread_hatch,
    "RAW": wread_hatch,
    "DQR": bread_hatch,
    "BallotRead": bread_hatch,
    "QuorumRead": qread_hatch,
    "Mixed": mixed_hatch,
    "EPaxos": epaxos_hatch,
    "EPaxos fast path": epaxos_hatch,
    "EPaxos slow path": epaxos_slow_hatch,
    "Etcd": "**",
}
