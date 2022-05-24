from data import col_post_deal



def upload_post_deal(post_deal):
    col_post_deal.insert_many([post_deal])