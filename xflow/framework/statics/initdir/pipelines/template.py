from xflow.framework.pipeline import Pipeline


class template(Pipeline):
    """
    pipeline 模版。
    """
    class Options(Pipeline.Options):
        """
        流水线参数表，根据需要自行定义。
        """
        pass

    def setup(self) -> None:
        """
        前置步骤。
        """
        self.options: __class__.Options  # 保留用于自动提示
        super().setup()

    def stage1(self) -> None:
        """
        阶段 1。
        每个 Pipeline 至少 1 个阶段，然后根据需要继续添加，命名为 stage2, stage3, ...。
        """
        pass

    def teardown(self) -> None:
        """
        后置步骤。
        """
        super().teardown()
