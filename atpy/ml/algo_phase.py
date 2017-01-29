from pyevents.events import *
from atpy.data.base_data_event import *


class AlgoPhase(object):
    """Simple training/testing/evaluation class"""

    def __init__(self, model, phase=None, default_listeners=None, event_type=BaseDataEvent):
        self.phase = phase
        self.model = model
        self.event_type = event_type
        self.iteration = 0

        if default_listeners is not None:
            self.before_iteration += default_listeners
            self.after_iteration += default_listeners
            default_listeners += self.new_input

    def train(self, data):
        self.iteration += 1
        self.before_iteration(data)
        model_output = self.model(data)
        self.after_iteration(data, model_output)

    def new_input(self, event):
        if isinstance(event, BaseDataEvent) and event.phase == self.phase and event.data is not None:
            self.train(event.data)

    @after
    def before_iteration(self, input_data):
        return BeforeIterationEvent(self.model, self.phase, self.iteration, input_data)

    @after
    def after_iteration(self, input_data, model_output):
        return AfterIterationEvent(self.model, self.phase, self.iteration, input_data, model_output)


class BeforeIterationEvent(object):
    def __init__(self, model, phase, iteration, input_data):
        """
        :param model: model method
        :param phase: phase
        :param iteration: current iteration
        :param input_data: current input
        """
        self.model = model
        self.phase = phase
        self.iteration = iteration
        self.input_data = input_data


class AfterIterationEvent(object):
    def __init__(self, model, phase, iteration, input_data, model_output):
        """
        :param model: model method
        :param phase: phase
        :param iteration: current iteration
        :param input_data: current input
        :param model_output: current output
        """
        self.model = model
        self.phase = phase
        self.iteration = iteration
        self.input_data = input_data
        self.model_output = model_output
