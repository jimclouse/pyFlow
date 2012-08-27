#!/usr/bin/env python
"""
pyFlow is a workflow framework for etl processing using python
"""


class Flow():
    """
    A flow is contains one or more steps and/or flows
    """

    def __init__(self, name, description):
        self.name = name
        self.description = description
        self.flow = []

    def addStep(self, step):
        self.flow.append(step)

    def _execute(self):
        for _step in self.flow:
            try:
                _step._execute()
            except Exception as e:
                raise Exception(str(e))

    def run(self):
        self._execute()


class Step():
    """
    a step is the actual execution task in the workflow

    a step will call a python method, track its timing, log any errors,
    log the execution status, and return status back to the calling flow
    """

    def __init__(self, name, action, args=None):
        self.name = name
        self.action = action
        self.args = args

    def _execute(self):
        try:
            print "running " + str(self.action.__name__)
            if self.args is None:
                self.action()
            else:
                self.action(self.args)
        except Exception as e:
            raise Exception("Encountered an error running step %s. %s" % (self.name, str(e),))

    def run(self):
        self._execute()
