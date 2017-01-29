from pyevents.events import *
from atpy.data.base_data_event import *


class AlgoPhase(object):
    """Simple training/testing/evaluation class"""

    def __init__(self, model, phase=None, default_listeners=None, input_event_processor=None):
        self.phase = phase
        self.model = model
        self.iteration = 0

        if input_event_processor is not None:
            self.input_event_processor = input_event_processor
        else:
            self.input_event_processor = lambda event: event.data if isinstance(event, BaseDataEvent) and event.phase == self.phase else None

        if default_listeners is not None:
            self.before_iteration += default_listeners
            self.after_iteration += default_listeners
            default_listeners += self.onevent

    def train(self, data):
        self.iteration += 1
        self.before_iteration(data)
        model_output = self.model(data)
        self.after_iteration(data, model_output)

    def onevent(self, event):
        processed_input = self.input_event_processor(event)
        if processed_input is not None:
            self.train(processed_input)

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
