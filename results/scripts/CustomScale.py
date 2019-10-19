from numpy import log2
import matplotlib.pyplot as plt

from matplotlib import scale as mscale
from matplotlib import transforms as mtransforms


class CustomScale(mscale.ScaleBase):
    name = 'custom'

    def __init__(self, axis, **kwargs):
        mscale.ScaleBase.__init__(self)
        self.thresh = None #thresh

    def get_transform(self):
        return self.CustomTransform(self.thresh)

    def set_default_locators_and_formatters(self, axis):
        pass

    class CustomTransform(mtransforms.Transform):
        input_dims = 1
        output_dims = 1
        is_separable = True

        def __init__(self, thresh):
            mtransforms.Transform.__init__(self)
            self.thresh = thresh

        def transform_non_affine(self, a):
            return 1.004**(a/1.004)

        def inverted(self):
            return CustomScale.InvertedCustomTransform(self.thresh)

    class InvertedCustomTransform(mtransforms.Transform):
        input_dims = 1
        output_dims = 1
        is_separable = True

        def __init__(self, thresh):
            mtransforms.Transform.__init__(self)
            self.thresh = thresh

        def transform_non_affine(self, a):
            return log2(a)*(1.004/log2(1.004))

        def inverted(self):
            return CustomScale.CustomTransform(self.thresh)

