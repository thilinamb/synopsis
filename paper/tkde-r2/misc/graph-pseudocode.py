def fan_out_score(feature):
    sc = Synopsis.context()
    # Select all vertices for this feature
    vertices = sc.query('SELECT ' + str(feature))
    edges_out = 0
    for vertex in vertices:
        edges_out += vertex.num_neighbors()
    fan_out = edges_out / len(vertices)
    return fan_out

def compact_hierarchy():
    sc = Synopsis.context()
    scores = []
    for feature in sc.features:
        fan_out = fan_out_score(feature)
        scores.append((fan_out, feature))
    new_hierarchy = sort_ascending(scores)
    return new_hierarchy

def estimate_num_vertices(hierarchy):
    tot_vertices = 0 ; prev_plane_sz = 1
    for feature in hierarchy:
        plane_sz = feature[1] * prev_plane_sz
        tot_vertices += size
        prev_plane = plane_sz

# Retrieve a sorted hierarchy configuration
hierarchy = compact_hierarchy()
min = estimate_num_vertices(hierarchy)
max = estimate_num_vertices(hierarchy.reverse())
