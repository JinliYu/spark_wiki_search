package action;

import com.opensymphony.xwork2.ActionSupport;
import com.opensymphony.xwork2.ActionContext;
import org.apache.struts2.ServletActionContext;
import javax.servlet.http.HttpServletRequest;

public class SearchAction extends ActionSupport {
    private String keywords;

    public String getKeywords() {
        return keywords;
    }

    public void setKeywords(String keywords) {
        this.keywords = keywords;
    }


    @Override
    public String execute(){
//        System.out.println(this.keywords);
        ActionContext ctx = ActionContext.getContext();
        HttpServletRequest request = (HttpServletRequest) ctx
                .get(ServletActionContext.HTTP_REQUEST);
        request.setAttribute("keywords", "You searched " + this.keywords);
        return "success";
    }

    public String searchResult(String search) {
        return "";
    }
}