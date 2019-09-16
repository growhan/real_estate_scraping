import pandas as pd
import numpy as np
import heapq
import math
import json
import datetime 
from scipy.spatial import ConvexHull, convex_hull_plot_2d

FILE_NAME = "Live_Street_Address_Management_SAM_Addresses.csv"
MAX_DIST = 0.07

class AddressClustering:
    def __init__(self, spread_sheet_name=FILE_NAME):
        self.spread_sheet_name = spread_sheet_name
        self.total_df = pd.read_csv(spread_sheet_name, low_memory=False)
        self.cluster_data = {}
        self.hulls = {}
        self.non_zero_missed = {}

    def cluster(self, cluster_count):
        df_points = self.total_df[["X","Y"]]
        sample_dict = {}    
        clusters = self.createClusters(cluster_count)
        cluster_hash_file_name = "out_cluster_hash_{}.txt".format(str(cluster_count))
        cluster_hash_file_csv = "out_cluster_hash_{}.csv".format(str(cluster_count))
        with open(cluster_hash_file_name, "w") as file:
            print(clusters["clusters_raw"], file=file)
        clusters["cluster_df"].to_csv(cluster_hash_file_csv)
        self.cluster_data[cluster_count] = clusters
        cluster_count = {}
        for key in self.addToClustercluster_data[cluster_count]["clusters_raw"].keys():
            cluster_count[key] = len(self.cluster_data[cluster_count]["clusters_raw"][key])
        out = self.shapes(cluster_count)
        self.hulls = out["hulls"]
        tiny_squares = out["tiny"]
        large_polygons = self.buildLargePolygons()
        with open("large_shapes.txt", "w") as file:
            print(large_polygons, file=file)
        with open("tiny_shapes.txt", "w") as file:
            print(tiny_squares, file=file)

    def getTinySquares(self, non_zero_missed):
        tiny_squares = {}
        for clus in non_zero_missed.keys():
            cluster_items = non_zero_missed[clus][["x", "y"]]
            temp = self.total_df.loc[clus][["X", "Y"]].values
            clus_coord = [temp[0], temp[1]]
            cluster_items.loc[len(df)] = clus_coord
            cluster_items.drop_duplicates()
            tiny_squares[clus] = []
            for coord in cluster_items.values:
                tiny_squares[clus].append(self.polyToHttp(self.buildTinySquare(coord)))
        return tiny_squares

    def buildTinySquare(self, coord):
        coords = []
        for op in operations:
            new_coord = [(coord[0] + (op[0] * degs)), (coord[1] + (op[1] * degs))]
            coords.append(new_coord)
        coords += [coords[0]]
        return coords

    def polyToHttp(self, polygon_vertices):
        temp = []
        for i in polygon_vertices:
            temp.append(str(i[0]) + "%20" + str(i[1]))
        return "%2C".join(temp)

    def buildLargePolygons(self):
        large_polygons = {}   
        for cluster in self.hulls.keys():
            simplice_data = self.hulls[cluster_a]
            path = []
            for vertex in simplice_data[0].vertices:
                new_coord = [float(simplice_data[1][vertex, 0]), float(simplice_data[1][vertex, 1])]
                path.append(new_coord)
            path += [path[0]]
            large_polygons[cluster] =self. polyToHttp((path))
        return large_polygons    

    def shapes(self, cluster_count):
        hulls = {}
        one_d = {}
        break_count = 0
        cluster_group_a = list(self.cluster_data[cluster_count]["cluster_df"]["cluster"])
        for idx, val in enumerate(cluster_group_a):
            print(idx)
            t_df = self.cluster_data[cluster_count]["cluster_df"][self.cluster_data[cluster_count]["cluster_df"].cluster == val]
            stats = t_df.describe()
            clean_t = t_df[(((abs(t_df["x"] - stats["x"]["mean"])) / stats["x"]["std"]) < 3) &
                        (((abs(t_df["y"] - stats["y"]["mean"])) / stats["y"]["std"]) < 3)]
            vals = clean_t[["x", "y"]].values
            shape_vals = vals.shape
            if clean_t.describe()["x"]["count"] <= 2 or shape_vals[0] <= 2 or shape_vals[1] < 2:
                continue
            new_hull = []
            try:
                new_hull = ConvexHull(vals)
            except:
                print("broken")
                print(val)
                one_d[val] = t_df[["X", "Y"]].values
                continue
            outliers = t_df[(((abs(t_df["x"] - stats["x"]["mean"])) / stats["x"]["std"]) >= 3) &
                    (((abs(t_df["y"] - stats["y"]["mean"])) / stats["y"]["std"]) >= 3)]
            one_d[val] = outliers
            hulls[val] = [new_hull, vals]
        return {"hulls" : hulls, "tiny": self.findTiny(one_d) }

    def findTiny(self, one_d):
        non_zero_missed = {}
        tot_missed = 0
        for cluster in one_d.keys():
            if len(one_d[cluster]) > 0:
                non_zero_missed[cluster] = one_d[cluster]
                tot_missed += len(non_zero_missed[cluster])
        return self.getTinySquares(non_zero_missed)

    def distance(self, cluster, x, y):
        return math.sqrt(((cluster[0] - x) ** 2) + ((cluster[1] - y) ** 2))

    def distanceAway(self, x, y, clusters):
        cluster_dist = []
        for cluster in clusters.keys():
            cluster_dist.append((distance(clusters[cluster][0], x, y), cluster, (x, y)))
        heapq.heapify(cluster_dist)    
        return cluster_dist

    def addToCluster(self, closest_clusters, x, y, clusters, max_clusters, rows):
        considred_heaps = [heapq.heappop(closest_clusters)]
        while(len(clusters[considred_heaps[-1][1]]) >= max_clusters):
            if considred_heaps[-1][0] > MAX_DIST:
                break
            considred_heaps.append(heapq.heappop(closest_clusters))
        clusters[considred_heaps[-1][1]].append(considred_heaps[-1][2])
        row = pd.DataFrame([[x, y, considred_heaps[-1][1]]], columns=["x", "y", "cluster"])
        rows.append(row)
        return cluster_df

    def createClusters(self, num_clusters):
        clusters = {}
        cluster_df = pd.DataFrame(columns=["x", "y", "cluster"])
        rows = []
        max_cluster_count = math.ceil(len(df_points.index) / num_clusters)
        samp = self.getSamples(num_clusters)
        for index, row in samp.iterrows():
            clusters[index] = [(row["X"], row["Y"])]
        for index, row in df_points.iterrows():
                print(index)
                x = row["X"]
                y = row["Y"]
                closest_clusters = distanceAway(x, y, clusters)
                cluster_df = addToCluster(closest_clusters, x, y, clusters, max_cluster_count, rows)
        cluster_df = pd.concat(rows)
        return {"clusters_raw": clusters, "cluster_df": cluster_df}
