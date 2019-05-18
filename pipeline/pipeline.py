import csv
import io
import itertools
import os
import pickle
import types
from collections import deque
from datetime import datetime

OUTPUT = os.getenv("HN_PIPELINE__CACHE_PATH")


class DAG:
    def __init__(self):
        self.graph = {}

    def add(self, node, to=None):
        self.graph.setdefault(node, [])
        if to is not None:
            self.graph.setdefault(to, [])
            self.graph[node].append(to)
        if len(self.sort()) != len(self.graph):
            # TODO make this exception more explicit
            raise Exception("Failed to add node to DAG")

    def in_degrees(self):
        # compute how many nodes point to a given node
        result = {}
        for parent, children in self.graph.items():
            result.setdefault(parent, 0)
            for child in children:
                result.setdefault(child, 0)
                result[child] += 1

        return result

    def sort(self):
        # topological sort
        in_degrees = self.in_degrees()
        q = deque()
        for node, in_degree in in_degrees.items():
            if in_degree == 0:
                q.append(node)

        found = []
        while q:
            node = q.popleft()
            for child in self.graph[node]:
                in_degrees[child] -= 1
                if in_degrees[child] == 0:
                    q.append(child)
            found.append(node)

        return found


class Pipeline:
    def __init__(self):
        self.tasks = DAG()

    def task(self, depends_on=None):
        # decorator method, creates a new task and add to the DAG
        def inner(f):
            if depends_on is not None:
                self.tasks.add(depends_on, f)
            else:
                self.tasks.add(f)
            return f

        return inner

    def run(self):
        scheduled = self.tasks.sort()
        completed = {}

        for task in scheduled:
            filename = os.path.join(OUTPUT, "{}.pickle".format(task.__name__))
            if os.path.exists(filename):
                with open(filename, "rb") as fp:
                    completed[task] = pickle.load(file=fp)
            else:
                for node, children in self.tasks.graph.items():
                    if task in children:
                        completed[task] = task(completed[node])
                if task not in completed:
                    completed[task] = task()
                if not isinstance(completed[task], types.GeneratorType):
                    with open(filename, "wb") as fp:
                        pickle.dump(completed[task], file=fp)

        return completed


def build_csv(lines, header=None, fp=None):
    if header is not None:
        lines = itertools.chain([header], lines)
    writer = csv.writer(fp, delimiter=',')
    writer.writerows(lines)
    fp.seek(0)

    return fp
