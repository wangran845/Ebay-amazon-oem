from __future__ import annotations

class Config:
    def __init__(self):
        self.Product_keywords = 'turbocharger_WXBSWE(5)'

    @property
    def product_user_1(self):
        product_user_1 = self.Product_keywords.replace(' ', '+')
        return product_user_1

    @property
    def product_user_2(self):
        product_user_2 = self.Product_keywords.replace(' ', '_')
        return product_user_2
