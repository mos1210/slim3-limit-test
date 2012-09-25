package slim3.controller;

import org.slim3.controller.Controller;
import org.slim3.controller.Navigation;

import slim3.dao.SpotDao;

public class TestController extends Controller {

    @Override
    public Navigation run() throws Exception {
        
        String action = param("action");
        
        SpotDao dao = new SpotDao();
        dao.create(action);
        
        return forward("TestController.jsp");
    }
}
