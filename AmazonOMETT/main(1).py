from code.circle_number_replacer import CircleNumberReplacer
from code.excel_clean import ExcelCleaner
from code.secondary_processor import SecondaryProcessor
from code.html_formatter import HTMLFormatter
from code.image_path_updater import ImagePathUpdater
#from code.oem_deduplicator_pump import OEMDeduplicator          #根据需要和下面那一句来回切换
from code.oem_deduplicator import OEMDeduplicator
from code.final_formatter import FinalFormatter
from code.text_cleaner import TextCleaner
from code.duplicate_remover_url import DuplicateRemover
from code.magento_formatter import MagentoFormatter
from code.oem_usage_filter import OEMUsageFilter
from code.BT7274 import KLbt7274
from code.producturlgenerator import ProductUrlGenerator
# 方式2: 如需自定义路径
    # xxx.process(
    #     input_file="/xxx/xxx.xlsx",
    #     output_file="/xxx/xxx.xlsx"
    # )


if __name__ == "__main__":

    DuplicateRemover().run()

    ExcelCleaner().run()

    SecondaryProcessor().run()

    HTMLFormatter().run()

    ImagePathUpdater().run()

    OEMDeduplicator().run()

    FinalFormatter().run()

    CircleNumberReplacer().run()

    TextCleaner().run()

    MagentoFormatter().run()

    OEMUsageFilter().run()

    KLbt7274().run()

    ProductUrlGenerator().run()

