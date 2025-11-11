from xflow.framework.pipeline import Pipeline


class example(Pipeline):
    """
    pipeline 示例。
    """
    def setup(self) -> None:
        """
        前置步骤。
        """
        super().setup()

    def stage1(self) -> None:
        """
        拉取代码。
        """
        self.node.exec('git clone https://github.com/zhaowcheng/xflow.framework.git')

    def stage2(self) -> None:
        """
        编译代码。
        """
        with self.node.cd('./xflow.framework'):
            self.node.exec('pip install -r requirements.txt')
            self.node.exec('pyinstaller --onedir xflow/framework/main.py')

    def stage3(self) -> None:
        """
        打包。
        """
        with self.node.cd('./xflow.framework'):
            self.node.exec('tar czvf xflow.tar.gz build/dist/')
            self.node.getfile(
                self.node.cwd.join('xflow.framework/xflow.tar.gz'),
                self.cwd
            )

    def teardown(self) -> None:
        """
        后置步骤。
        """
        super().teardown()
        self.node.exec(f'rm -rf ./xflow.framework')
