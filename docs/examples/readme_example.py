# %%
import pandas as pd

from networkframe import NetworkFrame

nodes = pd.DataFrame(
    {
        "name": ["A", "B", "C", "D", "E"],
        "color": ["red", "blue", "blue", "red", "blue"],
    },
    index=[0, 1, 2, 3, 4],
)
edges = pd.DataFrame(
    {
        "source": [0, 1, 2, 2, 3],
        "target": [1, 2, 3, 1, 0],
        "weight": [1, 2, 3, 4, 5],
    }
)

nf = NetworkFrame(nodes, edges)
print(nf)

# %%

# Select a subgraph by node color
red_nodes = nf.query_nodes("color == 'red'")
print(red_nodes.nodes)

# %%

# Select a subgraph by edge weight
strong_nf = nf.query_edges("weight > 2")
print(strong_nf.edges)

# %%

# Iterate over subgraphs by node color
for color, subgraph in nf.groupby_nodes("color", axis="both"):
    print(color)
    print(subgraph.edges)

# %%

# Apply node information to edges
# (e.g. to color edges by the color of their source node)

nf.apply_node_features("color", inplace=True)
print(nf.edges)
