package flinklearning._11connection;

public class Xyz {

    private int sort(int[] arr, int st, int ed) {
        int point = st;
        while (st < ed) {
            while (st < ed && arr[ed] >= arr[point]) {
                ed--;
            }
            while (st < ed && arr[st] <= arr[point]) {
                st++;
            }

            if (st < ed) {
                int temp = arr[st];
                arr[st] = arr[ed];
                arr[ed] = temp;
            }
        }

        int temp = arr[st];
        arr[st] = arr[point];
        arr[point] = temp;
        return st;
    }

    private void quick(int[] arr, int st, int ed) {
        int point;
        if (st < ed) {
            point = sort(arr, st, ed);
            quick(arr, st, point - 1);
            quick(arr, point + 1, ed);
        }

    }


    public static void main(String[] args) {
        int[] arr = {5, 6, 0, 0, 1, 3, 4};
        Xyz xyz = new Xyz();

        xyz.quick(arr, 0, arr.length - 1);
        System.out.println("-------");
    }
}
